package com.ontotext.trree.plugin.mongodb;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import com.ontotext.test.utils.StandardUtils;

public class TestPluginMongoExportTypes extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
		addMongoDates();
	}

	@Test
	public void testGetResultFromDocumentById_CanonicalLong() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/altText> ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetResultFromDocumentById_CanonicalDouble() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/altText1> ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetResultFromDocumentById_CanonicalInt() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/altText2> ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetResultFromDocumentById_CanonicalArray() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/altText3> ?o .\n"
				+ "\t}\n"
				+ "}";

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
