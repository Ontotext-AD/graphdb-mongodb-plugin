package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoBasicQueries extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
		addMongoDates();
	}

	@Test
	public void testGetResultsFromDocumentById() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testAggregation() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:aggregate \"[{'$match': {}}, {'$sort': {'@id': 1}}, {'$limit': 2}]\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyOrderedResult();
	}

	@Test
	public void testGetResultsFromDocumentByDistinctFields() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?p ?o {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{\"@graph.@type\" : \"cwork:Programme\", '@graph.cwork:about.@id' : \"dbpedia:Brasserie_du_Bocq\" , \"@graph.cwork:category.@id\" : \"http://www.bbc.co.uk/category/Company\"}''' ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testFilterDocumentsByCreationDate() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\n" +
				"PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>\n" +
				"select ?creativeWork ?dateCreated ?title ?liveCoverage ?audience {\n" +
				"   ?search a inst:spb100 ;\n" +
				"           :find \"{'@graph.cwork:dateCreated.@date': {'$gt': ISODate('2011-08-18T17:22:42.895Z'), '$lt': ISODate('2011-08-20T07:27:15.927Z')  }, '@graph.@type': 'cwork:BlogPost'}\" ;\n" +
				"           :entity ?entity .\n" +
				"   graph inst:spb100 {\n" +
				"       ?creativeWork cwork:dateCreated ?dateCreated .\n" +
				"       ?creativeWork cwork:title ?title .\n" +
				"       ?creativeWork cwork:category ?category .\n" +
				"       ?creativeWork cwork:liveCoverage ?liveCoverage .\n" +
				"       ?creativeWork cwork:audience ?audience .\n" +
				"   }\n" +
				"}";

		verifyUnorderedResult();
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
