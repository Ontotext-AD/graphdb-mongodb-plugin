# GraphDB MongoDB Plugin

This is the GraphDB MongoDB plugin.

## Building the plugin

The plugin is a Maven project.

Run `mvn clean package` to build the plugin and execute the tests.

The built plugin can be found in the `target` directory:

- `mongodb-plugin-graphdb-plugin.zip`

## Installing the plugin

External plugins are installed under `lib/plugins` in the GraphDB distribution
directory. To install the plugin follow these steps:

1. Remove the directory containing another version of the plugin from `lib/plugins` (e.g. `mongodb-plugin`).
2. Unzip the built zip file in `lib/plugins`.
3. Restart GraphDB. 

## Releases & Branches

### Master

Currently the `master` branch is used for releases compatible with the latest versions of GraphDB. This means that the
changes should be compatible with the GraphDB SDK, Java, RDF4J, etc.

### Releases/GraphDB-10.8

There are protected branches prefixed `releases/graphdb-10.8-*`, which is used for plugin releases that have to be
compatible with GraphDB 10.8.

These branches are compatible with older version of the GraphDB SDK, Java 11 and RDF4J 4.

The need for such branches comes from the fact that we still support some of the older GraphDB versions and sometimes we
have to port a fix or functionality required by clients.
