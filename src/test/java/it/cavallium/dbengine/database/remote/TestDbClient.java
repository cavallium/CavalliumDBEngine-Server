package it.cavallium.dbengine.database.remote;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import java.net.InetSocketAddress;

public class TestDbClient {

	public static LLQuicConnection create(BufferAllocator allocator,
			CompositeMeterRegistry meterRegistry,
			InetSocketAddress serverAddress) {
		return new LLQuicConnection(allocator,
				meterRegistry,
				new InetSocketAddress(0),
				serverAddress
		);
	}
}
