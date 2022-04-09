package it.cavallium.dbengine.database.server;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 *
 * @param <I> Identifier type
 * @param <V> Value type
 */
public class ReferencedResources<I, E, V> {

	private final ResourceGetter<I, E, V> resourceGetter;
	private final ResourceFreer<V> resourceFree;
	private final AtomicLong nextReferenceId = new AtomicLong(1);
	private final ConcurrentHashMap<I, Long> identifierToReferenceId = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Long, I> identifierByReferenceId = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Long, V> resourceByReferenceId = new ConcurrentHashMap<>();

	public ReferencedResources(ResourceGetter<I, E, V> resourceGetter, ResourceFreer<V> resourceFree) {
		this.resourceGetter = resourceGetter;
		this.resourceFree = resourceFree;
	}

	protected Mono<? extends V> obtain(I identifier, E extraParams) {
		return resourceGetter.obtain(identifier, extraParams);
	}

	protected Mono<Void> free(V resource) {
		return resourceFree.free(resource);
	}

	public Mono<Long> getReference(I identifier, @Nullable E extraParams) {
		Mono<Long> existingDbMono = Mono.fromSupplier(() -> identifierToReferenceId.get(identifier));

		if (extraParams != null) {
			// Defer to avoid building this chain when not needed
			Mono<Long> dbCreationMono = Mono.defer(() -> this
					.obtain(identifier, extraParams)
					.flatMap(db -> Mono.fromCallable(() -> {
						long referenceId = nextReferenceId.getAndIncrement();
						resourceByReferenceId.put(referenceId, db);
						identifierToReferenceId.put(identifier, referenceId);
						identifierByReferenceId.put(referenceId, identifier);
						return referenceId;
					})));

			return existingDbMono.switchIfEmpty(dbCreationMono);
		} else {
			return existingDbMono
					.switchIfEmpty(Mono.error(() -> new NoSuchElementException("Resource not found: " + identifier)));
		}
	}

	public Mono<ReferencedResource<V>> getResource(I identifier, @Nullable E extraParams) {
		Mono<ReferencedResource<V>> existingDbMono = Mono.fromSupplier(() -> {
			var referenceId = identifierToReferenceId.get(identifier);
			if (referenceId == null) {
				return null;
			}
			var resource = resourceByReferenceId.get(referenceId);
			if (resource == null) {
				return null;
			}
			return new ReferencedResource<>(referenceId, resource);
		});

		if (extraParams != null) {
			// Defer to avoid building this chain when not needed
			Mono<ReferencedResource<V>> dbCreationMono = Mono.defer(() -> this
					.obtain(identifier, extraParams)
					.map(resource -> {
						long referenceId = nextReferenceId.getAndIncrement();
						resourceByReferenceId.put(referenceId, resource);
						identifierToReferenceId.put(identifier, referenceId);
						identifierByReferenceId.put(referenceId, identifier);
						return new ReferencedResource<>(referenceId, resource);
					}));

			return existingDbMono.switchIfEmpty(dbCreationMono);
		} else {
			return existingDbMono
					.switchIfEmpty(Mono.error(() -> new NoSuchElementException("Resource not found: " + identifier)));
		}
	}

	public Mono<V> getResource(long referenceId) {
		Mono<V> existingDbMono = Mono.fromSupplier(() -> resourceByReferenceId.get(referenceId));
		return existingDbMono.switchIfEmpty(Mono.error(() ->
				new NoSuchElementException("Resource not found: " + referenceId)));
	}

	public Mono<Void> releaseResource(I identifier) {
		return Mono.fromSupplier(() -> {
			var referenceId = identifierToReferenceId.remove(identifier);
			if (referenceId == null) {
				return null;
			}
			identifierByReferenceId.remove(referenceId);
			return resourceByReferenceId.remove(referenceId);
		}).flatMap(this::free);
	}

	public Mono<Void> releaseResource(long referenceId) {
		return Mono.<V>fromSupplier(() -> {
			var identifier = identifierByReferenceId.remove(referenceId);
			if (identifier == null) {
				return null;
			}
			identifierToReferenceId.remove(identifier);
			return resourceByReferenceId.remove(referenceId);
		}).flatMap(this::free);
	}
}
