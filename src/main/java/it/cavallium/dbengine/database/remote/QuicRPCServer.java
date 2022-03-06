package it.cavallium.dbengine.database.remote;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.DefaultBufferAllocators;
import io.net5.buffer.api.Send;
import io.netty.handler.ssl.ClientAuth;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicConnectionIdGenerator;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.LLSingleton;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCEventCodec;
import it.cavallium.dbengine.lucene.LuceneRocksDBManager;
import it.cavallium.dbengine.rpc.current.data.Binary;
import it.cavallium.dbengine.rpc.current.data.BinaryOptional;
import it.cavallium.dbengine.rpc.current.data.CloseDatabase;
import it.cavallium.dbengine.rpc.current.data.CloseLuceneIndex;
import it.cavallium.dbengine.rpc.current.data.Empty;
import it.cavallium.dbengine.rpc.current.data.GeneratedEntityId;
import it.cavallium.dbengine.rpc.current.data.GetDatabase;
import it.cavallium.dbengine.rpc.current.data.GetLuceneIndex;
import it.cavallium.dbengine.rpc.current.data.GetSingleton;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateEnd;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateInit;
import it.cavallium.dbengine.rpc.current.data.SingletonUpdateOldData;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableBinary;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.netty.Connection;
import reactor.netty.DisposableChannel;
import reactor.netty.incubator.quic.QuicServer;

public class QuicRPCServer {

	private static final Logger LOG = LogManager.getLogger(QuicRPCServer.class);
	private final QuicServer quicServer;
	protected final LuceneRocksDBManager rocksDBManager;
	protected final LLDatabaseConnection localDb;
	private final AtomicReference<Connection> connectionAtomicReference = new AtomicReference<>();

	private final ReferencedResources<String, GetDatabase, LLKeyValueDatabase> dbs = new ReferencedResources<>(this::obtainDatabase, LLKeyValueDatabase::close);
	private final ReferencedResources<DatabasePartName, GetSingleton, LLSingleton> singletons = new ReferencedResources<>(this::obtainSingleton, s -> Mono.empty());
	private final ReferencedResources<String, GetLuceneIndex, LLLuceneIndex> indices = new ReferencedResources<>(this::obtainLuceneIndex, LLLuceneIndex::close);

	public QuicRPCServer(LuceneRocksDBManager rocksDBManager, LLDatabaseConnection localDb, QuicServer quicServer) {
		this.rocksDBManager = rocksDBManager;
		this.localDb = localDb;
		this.quicServer = quicServer.handleStream((in, out) -> in
				.withConnection(conn -> conn.addHandler(new RPCEventCodec()))
				.receiveObject()
				.cast(RPCEvent.class)
				.log("ServerBoundRequest", Level.FINEST, SignalType.ON_NEXT)
				.switchOnFirst((Signal<? extends RPCEvent> first, Flux<RPCEvent> flux) -> {
					if (first.hasValue()) {
						ServerBoundRequest value = (ServerBoundRequest) first.get();
						if (value instanceof GetDatabase getDatabase) {
							return handleGetDatabase(getDatabase)
									.transform(this::catchRPCErrorsFlux);
						} else if (value instanceof GetSingleton getSingleton) {
							return handleGetSingleton(getSingleton)
									.transform(this::catchRPCErrorsFlux);
						} else if (value instanceof SingletonUpdateInit singletonUpdateInit) {
							return handleSingletonUpdateInit(singletonUpdateInit, flux.skip(1))
									.transform(this::catchRPCErrorsFlux);
						} else if (value instanceof CloseDatabase closeDatabase) {
							return handleCloseDatabase(closeDatabase)
									.transform(this::catchRPCErrorsFlux);
						} else if (value instanceof CloseLuceneIndex closeLuceneIndex) {
							return handleCloseLuceneIndex(closeLuceneIndex)
									.transform(this::catchRPCErrorsFlux);
						} else if (value instanceof GetLuceneIndex getLuceneIndex) {
							return handleGetLuceneIndex(getLuceneIndex)
									.transform(this::catchRPCErrorsFlux);
						} else {
							return QuicUtils.catchRPCErrors(new UnsupportedOperationException("Unsupported request type: " + first));
						}
					} else {
						return flux;
					}
				})
				.doOnError(ex -> LOG.error("Failed to handle a request", ex))
				.onErrorResume(QuicUtils::catchRPCErrors)
				.concatMap(response -> out
						.withConnection(conn -> conn.addHandler(new RPCEventCodec()))
						.sendObject(response)
				)
		);
	}

	private Flux<RPCEvent> catchRPCErrorsFlux(Publisher<? extends RPCEvent> flux) {
		return Flux
				.from(flux)
				.doOnError(ex -> LOG.error("Failed to handle a request", ex))
				.cast(RPCEvent.class)
				.onErrorResume(QuicUtils::catchRPCErrors);

	}

	public Mono<Void> bind() {
		return quicServer.bind().doOnNext(connectionAtomicReference::set).then();
	}

	public Mono<Void> dispose() {
		return Mono
				.fromSupplier(connectionAtomicReference::get)
				.doOnNext(DisposableChannel::dispose)
				.flatMap(DisposableChannel::onDispose);
	}

	public Mono<Void> onDispose() {
		return Mono.fromSupplier(connectionAtomicReference::get).flatMap(DisposableChannel::onDispose);
	}

	public static void main(String[] args) throws URISyntaxException {
		var rocksDBManager = new LuceneRocksDBManager();
		var localDb = new LLLocalDatabaseConnection(DefaultBufferAllocators.preferredAllocator(),
				new CompositeMeterRegistry(), Path.of("."), false, rocksDBManager);
		String keyFileLocation = System.getProperty("it.cavalliumdb.keyFile", null);
		String keyFilePassword = System.getProperty("it.cavalliumdb.keyPassword", null);
		String certFileLocation = System.getProperty("it.cavalliumdb.certFile", null);
		String clientCertsLocation = System.getProperty("it.cavalliumdb.clientCerts", null);
		String bindAddressText = Objects.requireNonNull(System.getProperty("it.cavalliumdb.bindAddress", null), "Empty bind address");
		var bindURI = new URI("inet://" + bindAddressText);
		var bindAddress = new InetSocketAddress(bindURI.getHost(), bindURI.getPort());

		QuicSslContext sslContext = QuicSslContextBuilder
				.forServer(new File(keyFileLocation), keyFilePassword, new File(certFileLocation))
				.trustManager(new File(clientCertsLocation))
				.applicationProtocols("db/0.9")
				.clientAuth(ClientAuth.REQUIRE)
				.build();
		var qs = reactor.netty.incubator.quic.QuicServer
				.create()
				.tokenHandler(InsecureQuicTokenHandler.INSTANCE)
				.bindAddress(() -> bindAddress)
				.secure(sslContext)
				.idleTimeout(Duration.ofSeconds(30))
				.connectionIdAddressGenerator(QuicConnectionIdGenerator.randomGenerator())
				.initialSettings(spec -> spec
						.maxData(10000000)
						.maxStreamDataBidirectionalLocal(1000000)
						.maxStreamDataBidirectionalRemote(1000000)
						.maxStreamsBidirectional(100)
						.maxStreamsUnidirectional(100)
				);
		QuicRPCServer server = new QuicRPCServer(rocksDBManager, localDb, qs);
		server.bind().block();
		server.onDispose().block();
		localDb.disconnect().block();
		rocksDBManager.closeAll();
	}

	private Flux<RPCEvent> handleSingletonUpdateInit(
			SingletonUpdateInit singletonUpdateInit,
			Flux<RPCEvent> otherRequests) {
		return singletons
				.getResource(singletonUpdateInit.singletonId())
				.flatMapMany(singleton -> {
					Many<RPCEvent> clientBound = Sinks.many().unicast().onBackpressureBuffer();
					Mono<RPCEvent> update = singleton.update(prev -> {
						clientBound
								.tryEmitNext(new SingletonUpdateOldData(prev != null, prev != null ? toByteList(prev) : ByteList.of()))
								.orThrow();
						SingletonUpdateEnd newValue = (SingletonUpdateEnd) otherRequests.singleOrEmpty().block();
						Objects.requireNonNull(newValue);
						if (!newValue.exist()) {
							return null;
						} else {
							return localDb.getAllocator().copyOf(QuicUtils.toArrayNoCopy(newValue.value()));
						}
					}, singletonUpdateInit.updateReturnMode())
							.map(result -> new BinaryOptional(result != null ? NullableBinary.of(Binary.of(toByteList(result))) : NullableBinary.empty()));
					return Flux.merge(update, clientBound.asFlux());
				});
	}

	private static ByteList toByteList(Send<Buffer> prev) {
		try (var prevVal = prev.receive()) {
			byte[] result = new byte[prevVal.readableBytes()];
			prevVal.readBytes(result, 0, result.length);
			return ByteList.of(result);
		}
	}

	private Mono<GeneratedEntityId> handleGetSingleton(GetSingleton getSingleton) {
		var id = new DatabasePartName(getSingleton.databaseId(), getSingleton.singletonListColumnName());
		return this.singletons.getReference(id, getSingleton).map(GeneratedEntityId::new);
	}

	private Mono<LLSingleton> obtainSingleton(DatabasePartName id, GetSingleton getSingleton) {
		Mono<LLKeyValueDatabase> dbMono = dbs.getResource(id.dbRef());
		return dbMono.flatMap(db -> db.getSingleton(
				QuicUtils.toArrayNoCopy(getSingleton.singletonListColumnName()),
				QuicUtils.toArrayNoCopy(getSingleton.name()),
				QuicUtils.toArrayNoCopy(getSingleton.defaultValue())
		));
	}

	private Mono<GeneratedEntityId> handleGetDatabase(GetDatabase getDatabase) {
		return this.dbs.getReference(getDatabase.name(), getDatabase).map(GeneratedEntityId::of);
	}

	private Mono<? extends LLKeyValueDatabase> obtainDatabase(String id, GetDatabase getDatabase) {
		// Disable optimistic transactions, since network transactions require a lot of time
		var options = getDatabase.databaseOptions().setOptimistic(false);
		return localDb.getDatabase(id, getDatabase.columns(), options);
	}

	public Mono<GeneratedEntityId> handleGetLuceneIndex(GetLuceneIndex getLuceneIndex) {
		return this.indices
				.getReference(getLuceneIndex.clusterName(), getLuceneIndex)
				.map(GeneratedEntityId::new);
	}

	private Mono<? extends LLLuceneIndex> obtainLuceneIndex(String id, GetLuceneIndex getLuceneIndex) {
		return localDb.getLuceneIndex(getLuceneIndex.clusterName(),
				getLuceneIndex.structure(),
				getLuceneIndex.indicizerAnalyzers(),
				getLuceneIndex.indicizerSimilarities(),
				getLuceneIndex.luceneOptions(),
				null
		);
	}

	private Mono<RPCEvent> handleCloseDatabase(CloseDatabase closeDatabase) {
		return this.dbs.getResource(closeDatabase.databaseId()).flatMap(LLKeyValueDatabase::close).thenReturn(Empty.of());
	}

	private Mono<RPCEvent> handleCloseLuceneIndex(CloseLuceneIndex closeLuceneIndex) {
		return this.indices
				.getResource(closeLuceneIndex.luceneIndexId())
				.flatMap(LLLuceneIndex::close)
				.thenReturn(Empty.of());
	}

}
