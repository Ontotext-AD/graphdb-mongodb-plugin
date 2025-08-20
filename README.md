# GraphDB MongoDB Plugin

This is the GraphDB MongoDB plugin.

## Building the plugin

The plugin is a Maven project.

Run `mvn clean package` to build the plugin and execute the tests.

The built plugin can be found in the `target` directory:

- `mongodb-plugin-graphdb-plugin.zip`

Note: The plugin tests require a GraphD 11 license in order to be run.
The license is expected to be placed in  `${user.home}/.graphdb/conf/graphdb.license`.
You can request a free license at: https://www.ontotext.com/products/graphdb/#try-graphdb

## Installing the plugin

External plugins are installed under `lib/plugins` in the GraphDB distribution
directory. To install the plugin follow these steps:

1. Remove the directory containing another version of the plugin from `lib/plugins` (e.g. `mongodb-plugin`).
1. Unzip the built zip file in `lib/plugins`.
1. Restart GraphDB. 
