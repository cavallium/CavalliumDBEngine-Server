package it.cavallium.dbengine.database.server;

import reactor.core.publisher.Mono;

public interface ResourceGetter<I, E, V> {

	Mono<? extends V> obtain(I identifier, E extra);
}
