package com.ontotext.trree.plugin.mongodb;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import com.ontotext.test.utils.StandardUtils;

public class TestPluginMongoCollationQueries extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
		addMongoDates();
	}

	@Test
	public void testAggregationWithCollate() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "            :aggregate '''[\n"
				+ "{\"$match\": {\"@graph.cwork:about.@id\": \"dbpedia:Jaime_Quesada_Chavarría\"}}\n"
				+ "]''' ;\n"
				+ "\t:collate \"{'locale': 'en','numericOrdering': true,'strength': 1 }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyOrderedResult();
	}

	@Test
	public void testGetResultsByDistinctFieldsWithCollation() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collate \"{'locale': 'en','numericOrdering': true,'strength': 1 }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyOrderedResult();
	}

	@Override
	protected boolean isLearnMode() {
		return false;
	}

	@Override
	protected RepositoryConfig createRepositoryConfiguration() {
		return StandardUtils.createOwlimSe("empty");
	}

}
