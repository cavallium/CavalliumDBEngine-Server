package it.cavallium.dbengine.database.server;

import it.unimi.dsi.fastutil.bytes.ByteList;

public record DatabasePartName(long dbRef, ByteList resourceName) {}
