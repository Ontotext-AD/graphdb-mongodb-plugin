# Changelog

## 1.2.0

- [GDB-11761](https://graphwise.atlassian.net/browse/GDB-11761): Adds batch processing of the result documents

  Added functionality where all documents matching the input query, up to a maximum are loaded in memory and returned as
  named graph collection instead of processing one by one.
  When the new functionality is used the plugin is no longer streaming but will require a buffer to store the documents
  until the query is complete.

  Added system property configuration that controls the maximum allowed batch size: `graphdb.mongodb.maxBatchSize`. This
  is to prevent Out-of-Memory problems.

  Updated the GraphDB SDK to stable a version and the Java version to 21.

## 1.1.0

- Makes the plugin compatible with GraphDB 11 and the new version of RDF4J.
- Replaces the JSONLD library.

## 1.0.x

- Legacy versions used prior to GraphDB 11.
