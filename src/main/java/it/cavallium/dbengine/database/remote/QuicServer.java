package it.cavallium.dbengine.database.remote;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.noop.NoopMeter;
import io.net5.buffer.api.DefaultBufferAllocators;
import io.netty.handler.ssl.ClientAuth;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicConnectionIdGenerator;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCClientBoundResponseDecoder;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCServerBoundRequestDecoder;
import it.cavallium.dbengine.rpc.current.data.GeneratedEntityId;
import it.cavallium.dbengine.rpc.current.data.GetDatabase;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public class QuicServer {

	private static final Logger LOG = LogManager.getLogger(QuicServer.class);
	private static LLLocalDatabaseConnection localDb;

	private static final AtomicLong nextResourceId = new AtomicLong(1);
	private static final ConcurrentHashMap<String, Long> dbToId = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<Long, LLKeyValueDatabase> dbById = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Long> indexToId = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<Long, LLLuceneIndex> indexById = new ConcurrentHashMap<>();

	public static void main(String[] args) throws URISyntaxException {
		localDb = new LLLocalDatabaseConnection(DefaultBufferAllocators.preferredAllocator(),
				new CompositeMeterRegistry(), Path.of("."), false);
		String keyFileLocation = System.getProperty("it.cavalliumdb.keyFile", null);
		String keyFilePassword = System.getProperty("it.cavalliumdb.keyPassword", null);
		String certFileLocation = System.getProperty("it.cavalliumdb.certFile", null);
		String clientCertsLocation = System.getProperty("it.cavalliumdb.clientCerts", null);
		String bindAddress = Objects.requireNonNull(System.getProperty("it.cavalliumdb.bindAddress", null), "Empty bind address");
		var bindURI = new URI("inet://" + bindAddress);

		QuicSslContext sslContext = QuicSslContextBuilder
				.forServer(new File(keyFileLocation), keyFilePassword, new File(certFileLocation))
				.trustManager(new File(clientCertsLocation))
				.applicationProtocols("db/0.9")
				.clientAuth(ClientAuth.REQUIRE)
				.build();
		var qs = reactor.netty.incubator.quic.QuicServer
				.create()
				.tokenHandler(InsecureQuicTokenHandler.INSTANCE)
				.bindAddress(() -> new InetSocketAddress(bindURI.getHost(), bindURI.getPort()))
				.secure(sslContext)
				.idleTimeout(Duration.ofSeconds(30))
				.connectionIdAddressGenerator(QuicConnectionIdGenerator.randomGenerator())
				.initialSettings(spec -> spec
						.maxData(10000000)
						.maxStreamDataBidirectionalLocal(1000000)
						.maxStreamDataBidirectionalRemote(1000000)
						.maxStreamsBidirectional(100)
						.maxStreamsUnidirectional(100)
				)
				.handleStream((in, out) -> {
					var inConn = in.withConnection(conn -> conn.addHandler(new RPCClientBoundResponseDecoder()));
					var outConn = out.withConnection(conn -> conn.addHandler(new RPCServerBoundRequestDecoder()));
					return inConn
							.receiveObject()
							.cast(ServerBoundRequest.class)
							.log("req", Level.INFO, SignalType.ON_NEXT)
							.switchOnFirst((first, flux) -> {
								if (first.hasValue()) {
									var value = first.get();
									if (value instanceof GetDatabase getDatabase) {
										return handleGetDatabase(getDatabase);
									} else {
										return Mono.error(new UnsupportedOperationException("Unsupported request type: " + first));
									}
								} else {
									return flux;
								}
							})
							.doOnError(ex -> LOG.error("Failed to handle a request", ex))
							.onErrorResume(ex -> Mono.empty())
							.transform(outConn::sendObject);
				});
		var conn = qs.bindNow();
		conn.onDispose().block();
	}

	private static Mono<GeneratedEntityId> handleGetDatabase(GetDatabase getDatabase) {
		Mono<GeneratedEntityId> dbCreationMono = localDb
				.getDatabase(getDatabase.name(), getDatabase.columns(), getDatabase.databaseOptions())
				.flatMap(db -> Mono.fromCallable(() -> {
					long id = nextResourceId.getAndIncrement();
					dbById.put(id, db);
					dbToId.put(getDatabase.name(), id);
					return GeneratedEntityId.of(id);
				}));

		Mono<GeneratedEntityId> existingDbMono = Mono
				.fromSupplier(() -> dbToId.get(getDatabase.name()))
				.map(GeneratedEntityId::of);

		return existingDbMono.switchIfEmpty(dbCreationMono);
	}
}
