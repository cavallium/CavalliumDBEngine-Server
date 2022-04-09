package it.cavallium.dbengine.database.server;

public record ReferencedResource<T>(Long reference, T resource) {}
