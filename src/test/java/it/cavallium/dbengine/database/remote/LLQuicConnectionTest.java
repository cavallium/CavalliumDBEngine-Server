package it.cavallium.dbengine.database.remote;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.DefaultBufferAllocators;
import it.cavallium.data.generator.nativedata.Nullableboolean;
import it.cavallium.data.generator.nativedata.Nullabledouble;
import it.cavallium.data.generator.nativedata.Nullableint;
import it.cavallium.data.generator.nativedata.Nullablelong;
import it.cavallium.dbengine.client.IndicizerAnalyzers;
import it.cavallium.dbengine.client.IndicizerSimilarities;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.lucene.LuceneUtils;
import it.cavallium.dbengine.rpc.current.data.ByteBuffersDirectory;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.rpc.current.data.LuceneIndexStructure;
import it.cavallium.dbengine.rpc.current.data.LuceneOptions;
import it.unimi.dsi.fastutil.ints.IntList;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LLQuicConnectionTest {

	private BufferAllocator allocator;
	private CompositeMeterRegistry meterRegistry;
	private TestDbServer server;
	private LLDatabaseConnection client;

	@BeforeEach
	void setUp() {
		this.allocator = DefaultBufferAllocators.preferredAllocator();
		this.meterRegistry = new CompositeMeterRegistry();
		this.server = TestDbServer.create(allocator, meterRegistry);
		server.bind().block();
		this.client = TestDbClient.create(allocator, meterRegistry, server.address()).connect().block();
	}

	@AfterEach
	void tearDown() {
		if (client != null) {
			client.disconnect().block();
		}
		if (server != null) {
			server.dispose().block();
		}
	}

	@Test
	void getAllocator() {
		assertEquals(allocator, client.getAllocator());
	}

	@Test
	void getMeterRegistry() {
		assertEquals(meterRegistry, client.getMeterRegistry());
	}

	@Test
	void getDatabase() {
		var dbName = "test-temp-db";
		var singletonsColumnName = "singletons";
		var db = client.getDatabase(dbName,
				List.of(ColumnUtils.special(singletonsColumnName)),
				new DatabaseOptions(List.of(),
						List.of(),
						Map.of(),
						true,
						true,
						true,
						true,
						true,
						true,
						Nullableint.empty(),
						Nullablelong.empty(),
						Nullablelong.empty(),
						Nullableboolean.empty(),
						false
				)
		).blockOptional().orElseThrow();
		assertEquals(dbName, db.getDatabaseName());
		assertEquals(allocator, db.getAllocator());
		assertEquals(meterRegistry, db.getMeterRegistry());
		assertDoesNotThrow(() -> db.close().block());
	}

	@Test
	void getLuceneIndex() {
		var shardName = "test-lucene-shard";
		var index = client.getLuceneIndex(shardName,
				LuceneUtils.singleStructure(),
				IndicizerAnalyzers.of(),
				IndicizerSimilarities.of(),
				new LuceneOptions(Map.of(),
						Duration.ofSeconds(1),
						Duration.ofSeconds(1),
						false,
						new ByteBuffersDirectory(),
						Nullableboolean.empty(),
						Nullabledouble.empty(),
						Nullableint.empty(),
						Nullableboolean.empty(),
						Nullableboolean.empty(),
						false,
						100
				),
				null).blockOptional().orElseThrow();
		assertEquals(shardName, index.getLuceneIndexName());
		assertDoesNotThrow(() -> index.close().block());
	}
}