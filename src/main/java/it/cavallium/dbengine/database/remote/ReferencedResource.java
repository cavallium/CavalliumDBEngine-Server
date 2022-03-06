package it.cavallium.dbengine.database.remote;

public record ReferencedResource<T>(Long reference, T resource) {}
