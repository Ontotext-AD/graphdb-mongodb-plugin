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
	public void testAggregationWithCollationLowStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "            :aggregate '''[\n"
				+ "{\"$match\": {\"@graph.cwork:about.@id\": \"dbpedia:Jaime_Quesada_Chavarría\"}}\n"
				+ "]''' ;\n"
				+ "\t:collation \"{'locale': 'en', 'strength': 1 }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyOrderedResult();
	}

	@Test
	public void testAggregationWithCollationDefaultStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "            :aggregate '''[\n"
				+ "{\"$match\": {\"@graph.cwork:about.@id\": \"dbpedia:Jaime_Quesada_Chavarría\"}}\n"
				+ "]''' ;\n"
				+ "\t:collation \"{'locale': 'en'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyOrderedResult();
	}

	@Test
	public void testGetResultsByFieldWithCollationDefaultStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collation \"{'locale': 'en' }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyOrderedResult();
	}

	@Test
	public void testGetResultsByFieldWithCollationLowStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collation \"{'locale': 'en','strength': 1 }\" ;"
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
