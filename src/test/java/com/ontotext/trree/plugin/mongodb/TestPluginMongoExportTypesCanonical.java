package com.ontotext.trree.plugin.mongodb;

/**
 * Same as {@link TestPluginMongoExportTypesRelaxed} but the source document contains BSON canonical forms,
 * e.g. {"$numberLong":"2469909754111"} instead of the simple relaxed JSON-compatible 2469909754111.
 */
public class TestPluginMongoExportTypesCanonical extends TestPluginMongoExportTypesRelaxed {
}
