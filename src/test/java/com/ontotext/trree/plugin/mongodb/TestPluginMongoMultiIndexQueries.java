package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import java.util.stream.Stream;

/**
 * The tests will work with 2 mongo configurations pointing to the same database and collection
 * with the same data, but will test how 2 different queries interact with each other and return results.
 */
public class TestPluginMongoMultiIndexQueries extends AbstractMongoBasicTest {

  private static final String FIND_MULTIPLE_RESULTS = "             :find '{\"@graph.@type\" : \"cwork:BlogPost\"}';\n ";
  private static final String FIND_BY_ID_BBCC_1646461 = "                 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n";
  private static final String FIND_BY_ID_BBCC_1646453 = "                 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n";

  @Override
	protected void loadData() {
		loadFilesToMongo();
		addMongoDates();
	}

	@Test
	public void shouldWorkWithSequentialQueries() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + FIND_BY_ID_BBCC_1646461
            + "             :entity ?entity1 .\n"
            + "    ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :entity ?entity2 .\n"
            + "    GRAPH mongodb-index:spb100 {\n"
            + "        ?s1 a ?o1\n"
            + "    }\n"
            + "    GRAPH mongodb-index:spb200 {\n"
            + "        ?s2 a ?o2\n"
            + "    }\n"
            + "}";

		verifyOrderedResult();
	}

  @Test
  public void shouldWorkWithSequentialQueriesAndGraphsAfterThat() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + FIND_BY_ID_BBCC_1646461
            + "             :entity ?entity1 .\n"
            + "    GRAPH mongodb-index:spb100 {\n"
            + "        ?s1 a ?o1\n"
            + "    }\n"
            + "    ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :entity ?entity2 .\n"
            + "    GRAPH mongodb-index:spb200 {\n"
            + "        ?s2 a ?o2\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWithSequentialQueriesInUnion() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {\n"
            + "    {\n"
            + "        ?search1 a mongodb-index:spb100 ;\n"
            + FIND_BY_ID_BBCC_1646461
            + "                 :entity ?entity1 .\n"
            + "        GRAPH mongodb-index:spb100 {\n"
            + "            ?s1 a ?o1\n"
            + "        }\n"
            + "    } UNION {\n"
            + "        ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "                 :entity ?entity2 .\n"
            + "        GRAPH mongodb-index:spb200 {\n"
            + "            ?s2 a ?o2\n"
            + "        }\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWithSequentialQueries_multipleResults() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + FIND_MULTIPLE_RESULTS
            + "             :entity ?entity1 .\n"
            + "    ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :entity ?entity2 .\n"
            + "    GRAPH mongodb-index:spb100 {\n"
            + "        ?s1 a ?o1\n"
            + "    }\n"
            + "    GRAPH mongodb-index:spb200 {\n"
            + "        ?s2 a ?o2\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWithSequentialQueriesInUnion_multipleResults() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {\n"
            + "    {\n"
            + "        ?search1 a mongodb-index:spb100 ;\n"
            + FIND_MULTIPLE_RESULTS
            + "                 :entity ?entity1 .\n"
            + "        GRAPH mongodb-index:spb100 {\n"
            + "            ?s1 a ?o1\n"
            + "        }\n"
            + "    } UNION {\n"
            + "        ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "                 :entity ?entity2 .\n"
            + "        GRAPH mongodb-index:spb200 {\n"
            + "            ?s2 a ?o2\n"
            + "        }\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWithSequentialQueries_multipleResults_customGraph() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "SELECT ?s1 ?o1 ?s2 ?o2 WHERE {\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + FIND_MULTIPLE_RESULTS
            + "             :graph mongodb-index:spb500 ;\n "
            + "             :entity ?entity1 .\n"
            + "    ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :entity ?entity2 .\n"
            + "    GRAPH mongodb-index:spb500 {\n"
            + "        ?s1 a ?o1\n"
            + "    }\n"
            + "    GRAPH mongodb-index:spb200 {\n"
            + "        ?s2 a ?o2\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWithSequentialQueries_multipleResults_customGraph_sameResults() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
            + "SELECT ?id ?type ?altText ?category WHERE {\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :graph mongodb-index:spb500 ;\n "
            + "             :entity ?entity1 .\n"
            + "    ?search2 a mongodb-index:spb200 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :entity ?entity2 .\n"
            + "    GRAPH mongodb-index:spb500 {\n"
            + "        ?id a ?type\n"
            + "    }\n"
            + "    GRAPH mongodb-index:spb200 {\n"
            + "				?id cwork:altText ?altText .\n"
            + "				?id cwork:category ?category .\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWith_multipleGraphs()
          throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
            + "SELECT ?id ?type ?altText ?category WHERE {\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + FIND_BY_ID_BBCC_1646453
            + "             :graph mongodb-index:spb500 ;\n "
            + "             :entity ?entity1 .\n"
            + "    GRAPH mongodb-index:spb500 {\n"
            + "        ?id a ?type\n"
            + "    }\n"
            + "    GRAPH mongodb-index:spb500 {\n"
            + "				?id cwork:altText ?altText .\n"
            + "				?id cwork:category ?category .\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  @Test
  public void shouldWorkWith_multipleGraphs_queryInTheMiddle()
          throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
            + "PREFIX onto: <http://www.ontotext.com/>\n"
            + "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
            + "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
            + "SELECT ?id ?type ?altText ?category"
//            + " from onto:explain "
            + " WHERE {\n"
            + "    GRAPH mongodb-index:spb100 {\n"
            + "        ?id a ?type\n"
            + "    }\n"
            + "    ?search1 a mongodb-index:spb100 ;\n"
            + "             :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
            + "             :entity ?entity1 .\n"
            + "    GRAPH mongodb-index:spb100 {\n"
            + "				?id cwork:altText ?altText .\n"
            + "				?id cwork:category ?category .\n"
            + "    }\n"
            + "}";

    verifyOrderedResult();
  }

  ///-------------

//	@Test
//	public void reorderGraphPatternLaterInQuery() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>"
//						+ "PREFIX onto:<http://www.ontotext.com/>"
//						+ "SELECT ?s1 ?o1 ?entity1 "
//						+ " WHERE {"
//						+ "GRAPH mongodb-index:spb500 {?s1 a ?o1}"
//						+ "?search1 a mongodb-index:spb100 ;"
//						+ ":find \"{'@id' : 'bbcc:1646461#id'}\" ;"
//						+":graph mongodb-index:spb500 ;"
//						+ ":entity ?entity1 ."
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithConcurrentSubSelects() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "				{\n"
//						+ "						SELECT ?s1 ?o1 \n"
//						+ "						WHERE {\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "						}\n"
//						+ "				} \n"
//						+ "		} UNION {\n"
//						+ "				{\n"
//						+ "						SELECT ?s2 ?o2 \n"
//						+ "						WHERE {\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "						}\n"
//						+ "				}\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithConcurrentSubSelects_customInvertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "				{\n"
//						+ "						SELECT ?s1 ?o1 \n"
//						+ "						WHERE {\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "						}\n"
//						+ "				} \n"
//						+ "		} UNION {\n"
//						+ "				{\n"
//						+ "						SELECT ?s2 ?o2 \n"
//						+ "						WHERE {\n"
//						+ "								GRAPH mongodb-index:spb600 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb600 ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "						}\n"
//						+ "				}\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithConcurrentSubSelects_invertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "				{\n"
//						+ "						SELECT ?s1 ?o1 \n"
//						+ "						WHERE {\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "						}\n"
//						+ "				} \n"
//						+ "		} UNION {\n"
//						+ "				{\n"
//						+ "						SELECT ?s2 ?o2 \n"
//						+ "						WHERE {\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "						}\n"
//						+ "				}\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithUNION_invertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "		} UNION {\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithUNION_customInvertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "	 {\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "		} UNION {\n"
//						+ "								GRAPH mongodb-index:spb600 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb600 ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithUNION() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "		} UNION {\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithUNION_customGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "		} UNION {\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb600 ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "								GRAPH mongodb-index:spb600 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "		}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkWithDoubleGraphPatterns() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?s1 ?o1 ?category \n"
//						+ "WHERE {\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s2 cwork:category ?category\n"
//						+ "								}\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkDouble_invertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?s1 ?o1 ?category \n"
//						+ "WHERE {\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s2 cwork:category ?category\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkDouble_customInvertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?s1 ?o1 ?category \n"
//						+ "WHERE {\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s2 cwork:category ?category\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkForDouble_regularAndCustomInvertedGraphs() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?s1 ?o1 ?category \n"
//						+ "WHERE {\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								GRAPH mongodb-index:spb100 {\n"
//						+ "										?s2 cwork:category ?category\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void shouldWorkTwoIndexes_doubleCustomInvertedGraphs() throws Exception {
//
//		// the result is graph order dependent.
//		// If the graphs or the query blocks below are reordered then the result will be wrong
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX onto:<http://www.ontotext.com/>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?s1 ?o1 ?s2 ?o2 \n"
//						+ "WHERE {\n"
//						+ "								GRAPH mongodb-index:spb500 {\n"
//						+ "										?s1 a ?o1\n"
//						+ "								}\n"
//						+ "								GRAPH mongodb-index:spb600 {\n"
//						+ "										?s2 a ?o2\n"
//						+ "								}\n"
//						+ "								?search1 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646461#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb500 ;\n"
//						+ "												 :entity ?entity1 .\n"
//						+ "								?search2 a mongodb-index:spb100 ;\n"
//						+ "												 :find \"{'@id' : 'bbcc:1646453#id'}\" ;\n"
//						+ "												 :graph mongodb-index:spb600 ;\n"
//						+ "												 :entity ?entity2 .\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void evaluateModelPatternBeforeQuery() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?id ?type ?category ?audience\n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "				GRAPH mongodb-index:spb100 {\n"
//						+ "						?id cwork:category ?category .\n"
//						+ "						?id a ?type .\n"
//						+ "				}\n"
//						+ "		}union {\n"
//						+ "				GRAPH mongodb-index:spb100 {\n"
//						+ "						?id cwork:audience ?audience .\n"
//						+ "						?id a ?type .\n"
//						+ "				}\n"
//						+ "		}\n"
//						+ "		?search a mongodb-index:spb100 ;\n"
//						+ "						 :find '{\"@graph.@type\" : \"cwork:BlogPost\"}' ;\n"
//						+ "						 :entity ?entity .\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}
//
//	@Test
//	public void evaluateModelPatternBeforeQuery_noRdfType() throws Exception {
//
//		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n"
//						+ "PREFIX mongodb-index:<http://www.ontotext.com/connectors/mongodb/instance#>\n"
//						+ "PREFIX cwork:<http://www.bbc.co.uk/ontologies/creativework/>\n"
//						+ "SELECT ?id ?altText ?category ?audience\n"
//						+ "WHERE {\n"
//						+ "		{\n"
//						+ "				GRAPH mongodb-index:spb100 {\n"
//						+ "						?id cwork:altText ?altText .\n"
//						+ "						?id cwork:category ?category .\n"
//						+ "				}\n"
//						+ "		}union {\n"
//						+ "				GRAPH mongodb-index:spb100 {\n"
//						+ "						?id cwork:altText ?altText .\n"
//						+ "						?id cwork:audience ?audience\n"
//						+ "				}\n"
//						+ "		}\n"
//						+ "		?search a mongodb-index:spb100 ;\n"
//						+ "						 :find '{\"@graph.@type\" : \"cwork:BlogPost\"}' ;\n"
//						+ "						 :entity ?entity .\n"
//						+ "}";
//
//		verifyUnorderedResult();
//	}

	@Override
	protected boolean isLearnMode() {
		return false;
	}

  @Override protected Stream.Builder<String> indexes() {
    return super.indexes().add("spb200");
  }

  @Override
	protected RepositoryConfig createRepositoryConfiguration() {
		return StandardUtils.createOwlimSe("empty");
	}

}
