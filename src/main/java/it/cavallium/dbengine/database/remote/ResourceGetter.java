package it.cavallium.dbengine.database.remote;

import reactor.core.publisher.Mono;

public interface ResourceGetter<I, E, V> {

	Mono<? extends V> obtain(I identifier, E extra);
}
