package it.cavallium.dbengine.database.remote;

import io.netty.handler.ssl.ClientAuth;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class QuicServer {

	public static void main(String[] args) throws URISyntaxException {
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
				.port(bindURI.getPort())
				.host(bindURI.getHost())
				.tokenHandler(InsecureQuicTokenHandler.INSTANCE)
				.secure(sslContext)
				.handleStream((in, out) -> in
						.withConnection(conn -> conn.addHandler(new RPCServerBoundRequestDecoder()))
						.receiveObject()
						.doFirst(() -> {
							System.out.println("###################################Stream created");
						})
						.cast(ServerBoundRequest.class)
						.log()
						.then()
				);
		qs.warmup().block();
		var conn = qs.bindNow();
		conn.onDispose().block();
	}
}
