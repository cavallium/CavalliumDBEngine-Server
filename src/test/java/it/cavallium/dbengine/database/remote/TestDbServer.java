package it.cavallium.dbengine.database.remote;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicConnectionIdGenerator;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.memory.LLMemoryDatabaseConnection;
import it.cavallium.dbengine.lucene.LuceneRocksDBManager;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.incubator.quic.QuicServer;

public class TestDbServer extends QuicRPCServer {

	private static final InetSocketAddress BIND_ADDRESS = new InetSocketAddress(8080);

	public TestDbServer(LuceneRocksDBManager rocksDBManager, LLDatabaseConnection localDb, QuicServer quicServer) {
		super(rocksDBManager, localDb, quicServer);
	}

	public static TestDbServer create(BufferAllocator allocator, CompositeMeterRegistry meterRegistry) {
		var rocksDBManager = new LuceneRocksDBManager();
		var localDb = new LLMemoryDatabaseConnection(allocator, meterRegistry);
		SelfSignedCertificate selfSignedCert;
		try {
			selfSignedCert = new SelfSignedCertificate();
		} catch (CertificateException e) {
			throw new RuntimeException(e);
		}
		QuicSslContext sslContext = QuicSslContextBuilder
				.forServer(selfSignedCert.key(), null, selfSignedCert.cert())
				.applicationProtocols("db/0.9")
				.clientAuth(ClientAuth.NONE)
				.build();
		var qs = reactor.netty.incubator.quic.QuicServer
				.create()
				.tokenHandler(InsecureQuicTokenHandler.INSTANCE)
				.bindAddress(() -> BIND_ADDRESS)
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
		return new TestDbServer(rocksDBManager, localDb, qs);
	}

	public InetSocketAddress address() {
		return BIND_ADDRESS;
	}

	@Override
	public Mono<Void> dispose() {
		var closeManager = Mono
				.<Void>fromRunnable(rocksDBManager::closeAll)
				.subscribeOn(Schedulers.boundedElastic());
		return super.dispose().then(closeManager).then(localDb.disconnect());
	}
}
