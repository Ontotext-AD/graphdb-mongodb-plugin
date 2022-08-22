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
				"	 ?search a inst:spb100 ;\n" +
				"					 :find \"{'@graph.cwork:dateCreated.@date': {'$gt': ISODate('2011-08-18T17:22:42.895Z'), '$lt': ISODate('2011-08-20T07:27:15.927Z')	}, '@graph.@type': 'cwork:BlogPost'}\" ;\n" +
				"					 :entity ?entity .\n" +
				"	 graph inst:spb100 {\n" +
				"			 ?creativeWork cwork:dateCreated ?dateCreated .\n" +
				"			 ?creativeWork cwork:title ?title .\n" +
				"			 ?creativeWork cwork:category ?category .\n" +
				"			 ?creativeWork cwork:liveCoverage ?liveCoverage .\n" +
				"			 ?creativeWork cwork:audience ?audience .\n" +
				"	 }\n" +
				"}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetSomeResultsFromQueryButNotNPEWithOtherQuery(){

		query = "PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>"
		+ "SELECT * WHERE {GRAPH mongodb-index:metadata_audit {?s ?p1 ?o1}}";

		verifyResultsCount(query, 0);
	}

	@Test
	public void testGetSomeResultsFromQueryButNotNPEWithOtherQuery2(){

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>"
				+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>"
				+ "SELECT * WHERE {"
				+ "GRAPH mongodb-index:spb100 {?s a ?o1}"
				+ "?search a mongodb-index:spb100 ;"
				+ ":find \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ ":entity ?entity ."
				+ "}";

		verifyResultsCount(query, 1);
	}

	@Test
	public void shouldAssignGraphIdAutomatically() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {"
						+ "?search1 a mongodb-index:spb100 ;"
						+ ":find \"{'@id' : 'bbcc:1646461#id'}\" ;"
						+ ":entity ?entity1 ."
						+ "?search2 a mongodb-index:spb100 ;"
						+ ":find \"{'@id' : 'bbcc:1646453#id'}\" ;"
						+ ":graph mongodb-index:spb1001 ;"
						+ ":entity ?entity2 ."
						+ "GRAPH mongodb-index:spb100 {?s1 a ?o1}"
						+ "GRAPH mongodb-index:spb1001 {?s2 a ?o2}"
						+ "}";

		verifyOrderedResult();
	}

	@Test
	public void reorderGraphPatternLaterInQuery() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>"
						+ "PREFIX onto:<http://www.ontotext.com/>"
						+ "SELECT ?s1 ?o1 ?entity1 "
						+ " WHERE {"
						+ "GRAPH mongodb-index:spb500 {?s1 a ?o1}"
						+ "?search1 a mongodb-index:spb100 ;"
						+ ":find \"{'@id' : 'bbcc:1646461#id'}\" ;"
						+":graph mongodb-index:spb500 ;"
						+ ":entity ?entity1 ."
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithConcurrentSubSelects() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "		{\n"
						+ "				{\n"
						+ "						SELECT ?s1 ?o1 \n"
						+ "						WHERE {\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "						}\n"
						+ "				} \n"
						+ "		} UNION {\n"
						+ "				{\n"
						+ "						SELECT ?s2 ?o2 \n"
						+ "						WHERE {\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "						}\n"
						+ "				}\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithConcurrentSubSelects_customInvertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "		{\n"
						+ "				{\n"
						+ "						SELECT ?s1 ?o1 \n"
						+ "						WHERE {\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "						}\n"
						+ "				} \n"
						+ "		} UNION {\n"
						+ "				{\n"
						+ "						SELECT ?s2 ?o2 \n"
						+ "						WHERE {\n"
						+ "								GRAPH mongodb-index:spb600 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb600 ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "						}\n"
						+ "				}\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithConcurrentSubSelects_invertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "		{\n"
						+ "				{\n"
						+ "						SELECT ?s1 ?o1 \n"
						+ "						WHERE {\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "						}\n"
						+ "				} \n"
						+ "		} UNION {\n"
						+ "				{\n"
						+ "						SELECT ?s2 ?o2 \n"
						+ "						WHERE {\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "						}\n"
						+ "				}\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithUNION_invertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "		{\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "		} UNION {\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithUNION_customInvertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "	 {\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "		} UNION {\n"
						+ "								GRAPH mongodb-index:spb600 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb600 ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithUNION() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "		{\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "		} UNION {\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithUNION_customGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "		{\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "		} UNION {\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb600 ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "								GRAPH mongodb-index:spb600 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "		}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkWithDoubleGraphPatterns() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
						+ "SELECT ?s1 ?o1 ?category \n"
						+ "WHERE {\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s2 cwork:category ?category\n"
						+ "								}\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkDouble_invertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
						+ "SELECT ?s1 ?o1 ?category \n"
						+ "WHERE {\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s2 cwork:category ?category\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkDouble_customInvertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
						+ "SELECT ?s1 ?o1 ?category \n"
						+ "WHERE {\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s2 cwork:category ?category\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkForDouble_regularAndCustomInvertedGraphs() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
						+ "SELECT ?s1 ?o1 ?category \n"
						+ "WHERE {\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								GRAPH mongodb-index:spb100 {\n"
						+ "										?s2 cwork:category ?category\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void shouldWorkTwoIndexes_doubleCustomInvertedGraphs() throws Exception {

		// the result is graph order dependent.
		// If the graphs or the query blocks below are reordered then the result will be wrong

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
						+ "PREFIX onto:<http://www.ontotext.com/>\n"
						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
						+ "WHERE {\n"
						+ "								GRAPH mongodb-index:spb500 {\n"
						+ "										?s1 a ?o1\n"
						+ "								}\n"
						+ "								GRAPH mongodb-index:spb600 {\n"
						+ "										?s2 a ?o2\n"
						+ "								}\n"
						+ "								?search1 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb500 ;\n"
						+ "												 :entity ?entity1 .\n"
						+ "								?search2 a mongodb-index:spb100 ;\n"
						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
						+ "												 :graph mongodb-index:spb600 ;\n"
						+ "												 :entity ?entity2 .\n"
						+ "}";

		verifyUnorderedResult();
	}

  @Test
  public void evaluateModelPatternBeforeQuery() throws Exception {

    // the result is graph order dependent.
    // If the graphs or the query blocks below are reordered then the result will be wrong

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
            + "SELECT ?id ?type\n"
            + "WHERE {\n"
            + "    {\n"
            + "        GRAPH mongodb-index:spb100 {\n"
            + "            ?id a ?type\n"
            + "        }\n"
            + "    }union {\n"
            + "        GRAPH mongodb-index:spb100 {\n"
            + "            ?id cwork:category ?category\n"
            + "        }\n"
            + "    }\n"
            + "    ?search a mongodb-index:spb100 ;\n"
            + "             :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
            + "             :entity ?entity .\n"
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
