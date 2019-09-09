package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoExtended extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
	}

	/**
	 * We should test that the plugin handlers jsonlds with and without context. The ones with context have a "@graph"
	 * node in the beginning
	 */
	@Test
	public void testJsonLdWithAndWithoutContexts() {

		String query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyResultsCount(query, 2);
	}

	/**
	 * Custom fields can be added to the mongo doc as nodes in the "custom" node. They should be parsed separately
	 * as they are not part of the json-ld standard.
	 */
	@Test
	public void testCustomFields() throws Exception {
		query = "PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\n" +
				"PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n" +
				"select ?p ?o{\n" +
				"    ?search a inst:spb100 ;\n" +
				"            :aggregate '''[\n" +
				"{\"$match\": {\"@graph.@type\": \"cwork:NewsItem\"}}\n" +
				"{\"$count\": \"size\"}, \n" +
				"{\"$project\": {\"custom.size\": \"$size\", \"custom.halfSize\": {\"$divide\": [\"$size\", 2]}}}\n" +
				"]''' ;\n" +
				"     :entity ?entity .\n" +
				"    graph inst:spb100 {\n" +
				"        ?s ?p ?o .\n" +
				"    }\n" +
				"}";

		verifyUnorderedResult();
	}

	@Override
	protected RepositoryConfig createRepositoryConfiguration() {
		return StandardUtils.createOwlimSe("empty");
	}
	
}
