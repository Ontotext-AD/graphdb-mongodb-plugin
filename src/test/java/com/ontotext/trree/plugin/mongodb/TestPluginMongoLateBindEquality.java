package com.ontotext.trree.plugin.mongodb;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;
import com.ontotext.test.utils.StandardUtils;

/**
 * Regression test: ensure that late-bound :find query created via BIND(REPLACE()) still
 * yields results when an additional BIND equality appears inside the GRAPH pattern.
 */
public class TestPluginMongoLateBindEquality extends AbstractMongoBasicTest {

    @Override
    protected void loadData() {
        // Reuse the standard creative works dataset from the basic query tests.
        loadFilesToMongo(INPUT_DIR.resolve(TestPluginMongoBasicQueries.class.getSimpleName()).toFile());
        addMongoDates();
    }

    @Test
    public void testLateBoundQueryWithInGraphBindEquality() throws Exception {
        // Reuse existing document id logic from other tests (1646461) present in sample data
        // Use the same CURIE-based @id pattern as the basic bind3 regression test.
        query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n"
                + "PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n"
                + "select ?s ?o ?eq {\n"
                + "\tBIND(\"{'@id' : 'bbcc:##ID###id'}\" as ?queryPattern)."
                + "\tBIND(REPLACE(?queryPattern, '##ID##', '1646461') as ?query)."
                + "\t?search a inst:spb100 ;\r\n"
                + "\t\t:find\t?query;\r\n"
                + "\t\t:project '{ \"@context\": 1, \"cwork:about\": 1, \"@type\": 1, \"@id\" : 1, \"@graph\" : 1 }' ;\r\n"
                + "\t\t:entity ?entity .\r\n"
                + "\tgraph inst:spb100 {\r\n"
                + "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .\r\n"
                + "\t\tBIND((?s = <http://www.bbc.co.uk/things/1646461#id>) as ?eq)\r\n"
                + "\t}\n"
                + "}";
    // Expect same rows as basic bind3 test but now include the explicit ?eq boolean column.
    // Dedicated expected file asserts the boolean binding for each row.
    verifyResult("testGetResultsFromDocumentById_withEq", false);
    }

    @Test
    public void testLateBoundQueryWithInGraphBindEqualityAnonymousSearch() throws Exception {
        query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n"
                + "PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n"
                + "select ?s ?o {\n"
                + "\tBIND(\"{'@id' : 'bbcc:##ID###id'}\" as ?queryPattern)."
                + "\tBIND(REPLACE(?queryPattern, '##ID##', '1646461') as ?query)."
                + "\t[] a inst:spb100 ;\r\n"
                + "\t\t:find\t?query;\r\n"
                + "\t\t:project '{ \"@context\": 1, \"cwork:about\": 1, \"@type\": 1, \"@id\" : 1, \"@graph\" : 1 }' ;\r\n"
                + "\t\t:entity ?entity .\r\n"
                + "\tgraph inst:spb100 {\r\n"
                + "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .\r\n"
                + "\t\tBIND((?s = <http://www.bbc.co.uk/things/1646461#id>) as ?eq)\r\n"
                + "\t}\n"
                + "}";

        verifyResult("testGetResultsFromDocumentById", false);
    }

	@Test
	public void testLateBoundBatchQueryYieldsEmptyResult() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n"
				+ "PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n"
				+ "PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>\r\n"
				+ "select ?s ?o {\n"
				+ "\tBIND(\"{'@id' : 'bbcc:##ID###id'}\" as ?queryPattern).\n"
				+ "\tBIND(REPLACE(?queryPattern, '##ID##', 'missing') as ?query).\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t\t:find ?query ;\n"
				+ "\t\t:project '{ \"@context\": 1, \"cwork:about\": 1, \"@type\": 1, \"@id\" : 1, \"@graph\" : 1 }' ;\n"
				+ "\t\t:batchSize 1 ;\n"
				+ "\t\t:entity ?entity .\n"
				+ "\tGRAPH inst:spb100 {\n"
				+ "\t\t?s cwork:about ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyResult("testLateBoundBatchQueryYieldsEmptyResult", false);
	}

	@Test
	public void testGraphRedirectionLeavesSourceGraphEmpty() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n"
				+ "PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n"
				+ "select ?s ?o ?sOriginal ?oOriginal {\n"
				+ "\tBIND(\"{'@id' : 'bbcc:1646461#id'}\" as ?query).\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t\t:find ?query ;\n"
				+ "\t\t:project '{ \\\"@context\\\": 1, \\\"cwork:about\\\": 1, \\\"@type\\\": 1, \\\"@id\\\" : 1, \\\"@graph\\\" : 1 }' ;\n"
				+ "\t\t:graph inst:spbCustom ;\n"
				+ "\t\t:entity ?entity .\n"
				+ "\tGRAPH inst:spbCustom {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .\n"
				+ "\t}\n"
				+ "\tOPTIONAL {\n"
				+ "\t\tGRAPH inst:spb100 {\n"
				+ "\t\t\t?sOriginal <http://www.bbc.co.uk/ontologies/creativework/about> ?oOriginal .\n"
				+ "\t\t}\n"
				+ "\t}\n"
				+ "}";

		verifyResult("testGraphRedirectionLeavesSourceGraphEmpty", false);
	}

    @Override
    protected RepositoryConfig createRepositoryConfiguration() {
        return StandardUtils.createOwlimSe("empty");
    }
}
