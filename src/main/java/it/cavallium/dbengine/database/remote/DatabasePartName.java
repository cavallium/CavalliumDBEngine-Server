package it.cavallium.dbengine.database.remote;

import it.unimi.dsi.fastutil.bytes.ByteList;

public record DatabasePartName(long dbRef, ByteList resourceName) {}
