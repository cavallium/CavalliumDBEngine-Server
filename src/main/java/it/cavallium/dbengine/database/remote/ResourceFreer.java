package it.cavallium.dbengine.database.remote;

import reactor.core.publisher.Mono;

public interface ResourceFreer<V> {

	Mono<Void> free(V resource);
}
