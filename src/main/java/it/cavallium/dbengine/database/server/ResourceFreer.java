package it.cavallium.dbengine.database.server;

import reactor.core.publisher.Mono;

public interface ResourceFreer<V> {

	Mono<Void> free(V resource);
}
