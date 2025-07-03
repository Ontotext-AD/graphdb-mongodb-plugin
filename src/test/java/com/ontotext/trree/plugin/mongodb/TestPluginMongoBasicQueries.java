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

  @Override
  protected RepositoryConfig createRepositoryConfiguration() {
    return StandardUtils.createOwlimSe("empty");
  }

  @Test
  public void testGetResultsFromDocumentById() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?o {
          ?search a inst:spb100 ;
                  :find "{'@id': 'bbcc:1646461#id'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultsFromDocumentById_withValues() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?o {
          VALUES ?query {
            "{'@id': 'bbcc:1646461#id'}"
          } .
          ?search a inst:spb100 ;
                  :find  ?query;
                  :project '{"@context": 1, "cwork:about": 1, "@type": 1, "@id": 1, "@graph": 1}' ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .
          }
        }
        """;
    verifyResult("testGetResultsFromDocumentById", false);
  }

  @Test
  public void testGetResultsFromDocumentById_withBind() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?o {
          BIND("{'@id': 'bbcc:1646461#id'}" AS ?query).
          ?search a inst:spb100 ;
                  :find  ?query;
                  :project '{"@context": 1, "cwork:about": 1, "@type": 1, "@id": 1, "@graph": 1}' ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .
          }
        }
        """;
    verifyResult("testGetResultsFromDocumentById", false);
  }

  @Test
  public void testGetResultsFromDocumentById_withBind2() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?o {
          BIND("{'@id': 'bbcc:1646461#id'}" AS ?query) .
          BIND('{"@context": 1, "cwork:about": 1, "@type": 1, "@id" : 1, "@graph" : 1}' AS ?projection) .
          ?search a inst:spb100 ;
                  :find  ?query ;
                  :project ?projection ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .
          }
        }
        """;
    verifyResult("testGetResultsFromDocumentById", false);
  }

  @Test
  public void testAggregation() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          ?search a inst:spb100 ;
                  :aggregate "[{'$match': {}}, {'$sort': {'@id': 1}}, {'$limit': 2}]" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyOrderedResult();
  }

  @Test
  public void testGetResultsFromDocumentByDistinctFields() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?p ?o {
          BIND(rdf:type AS ?p) .
          ?search a inst:spb100 ;
                  :find '''
                    {
                      "@graph.@type": "cwork:Programme",
                      '@graph.cwork:about.@id': "dbpedia:Brasserie_du_Bocq",
                      "@graph.cwork:category.@id": "http://www.bbc.co.uk/category/Company"
                    }''' ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testFilterDocumentsByCreationDate() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?creativeWork ?dateCreated ?title ?liveCoverage ?audience {
          ?search a inst:spb100 ;
                  :find '''
                    {
                      '@graph.cwork:dateCreated.@date': {
                        '$gt': ISODate('2011-08-18T17:22:42.895Z'),
                        '$lt': ISODate('2011-08-20T07:27:15.927Z')
                      },
                      '@graph.@type': 'cwork:BlogPost'
                    }''' ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?creativeWork cwork:dateCreated ?dateCreated .
            ?creativeWork cwork:title ?title .
            ?creativeWork cwork:category ?category .
            ?creativeWork cwork:liveCoverage ?liveCoverage .
            ?creativeWork cwork:audience ?audience .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetSomeResultsFromQueryButNotNPEWithOtherQuery() {
    query = """
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        SELECT *
        WHERE {
          GRAPH mongodb-index:metadata_audit {
            ?s ?p1 ?o1 .
          }
        }
        """;
    verifyResultsCount(query, 0);
  }

  @Test
  public void testGetSomeResultsFromQueryButNotNPEWithOtherQuery2() {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT *
        WHERE {
          GRAPH mongodb-index:spb100 {
            ?s a ?o1 .
          }
          ?search a mongodb-index:spb100 ;
                  :find "{'@id': 'bbcc:1646461#id'}" ;
                  :entity ?entity .
        }
        """;
    verifyResultsCount(query, 1);
  }

  @Test
  public void shouldAssignGraphIdAutomatically() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :entity ?entity1 .
          ?search2 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646453#id'}" ;
                   :graph mongodb-index:spb1001 ;
                   :entity ?entity2 .
          GRAPH mongodb-index:spb100 {
            ?s1 a ?o1 .
          }
          GRAPH mongodb-index:spb1001 {
            ?s2 a ?o2 .
          }
        }
        """;
    verifyOrderedResult();
  }

  @Test
  public void reorderGraphPatternLaterInQuery() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?entity1
        WHERE {
          GRAPH mongodb-index:spb500 {
            ?s1 a ?o1 .
          }
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :graph mongodb-index:spb500 ;
                   :entity ?entity1 .
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithConcurrentSubSelects() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            {
              SELECT ?s1 ?o1
              WHERE {
                ?search1 a mongodb-index:spb100 ;
                         :find "{'@id': 'bbcc:1646461#id'}" ;
                         :entity ?entity1 .
                GRAPH mongodb-index:spb100 {
                  ?s1 a ?o1 .
                }
              }
            }
          } UNION {
            {
              SELECT ?s2 ?o2
              WHERE {
                ?search2 a mongodb-index:spb100 ;
                         :find "{'@id': 'bbcc:1646453#id'}" ;
                         :entity ?entity2 .
                GRAPH mongodb-index:spb100 {
                  ?s2 a ?o2 .
                }
              }
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithConcurrentSubSelects_customInvertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            {
              SELECT ?s1 ?o1
              WHERE {
                GRAPH mongodb-index:spb500 {
                  ?s1 a ?o1 .
                }
                ?search1 a mongodb-index:spb100 ;
                         :find "{'@id': 'bbcc:1646461#id'}" ;
                         :graph mongodb-index:spb500 ;
                         :entity ?entity1 .
              }
            }
          } UNION {
            {
              SELECT ?s2 ?o2
              WHERE {
                GRAPH mongodb-index:spb600 {
                  ?s2 a ?o2 .
                }
                ?search2 a mongodb-index:spb100 ;
                         :find "{'@id': 'bbcc:1646453#id'}" ;
                         :graph mongodb-index:spb600 ;
                         :entity ?entity2 .
              }
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithConcurrentSubSelects_invertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>
        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            {
              SELECT ?s1 ?o1
              WHERE {
                GRAPH mongodb-index:spb100 {
                  ?s1 a ?o1 .
                }
                ?search1 a mongodb-index:spb100 ;
                         :find "{'@id': 'bbcc:1646461#id'}" ;
                         :entity ?entity1 .
              }
            }
          } UNION {
            {
              SELECT ?s2 ?o2
              WHERE {
                GRAPH mongodb-index:spb100 {
                  ?s2 a ?o2 .
                }
                ?search2 a mongodb-index:spb100 ;
                         :find "{'@id': 'bbcc:1646453#id'}" ;
                         :entity ?entity2 .
              }
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithUNION_invertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            GRAPH mongodb-index:spb100 {
              ?s1 a ?o1 .
            }
            ?search1 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646461#id'}" ;
                     :entity ?entity1 .
          } UNION {
            GRAPH mongodb-index:spb100 {
              ?s2 a ?o2 .
            }
            ?search2 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646453#id'}" ;
                     :entity ?entity2 .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithUNION_customInvertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            GRAPH mongodb-index:spb500 {
              ?s1 a ?o1 .
            }
            ?search1 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646461#id'}" ;
                     :graph mongodb-index:spb500 ;
                     :entity ?entity1 .
          } UNION {
            GRAPH mongodb-index:spb600 {
              ?s2 a ?o2 .
            }
            ?search2 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646453#id'}" ;
                     :graph mongodb-index:spb600 ;
                     :entity ?entity2 .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithUNION() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            ?search1 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646461#id'}" ;
                     :entity ?entity1 .
            GRAPH mongodb-index:spb100 {
              ?s1 a ?o1 .
            }
          } UNION {
            ?search2 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646453#id'}" ;
                     :entity ?entity2 .
            GRAPH mongodb-index:spb100 {
              ?s2 a ?o2 .
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithUNION_customGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          {
            ?search1 a mongodb-index:spb100 ;
                     :find "{'@id': 'bbcc:1646461#id'}" ;
                     :graph mongodb-index:spb500 ;
                     :entity ?entity1 .
            GRAPH mongodb-index:spb500 {
              ?s1 a ?o1 .
            }
          } UNION {
            ?search2 a mongodb-index:spb100 ;
                     :find "{'@id' : 'bbcc:1646453#id'}" ;
                     :graph mongodb-index:spb600 ;
                     :entity ?entity2 .
            GRAPH mongodb-index:spb600 {
              ?s2 a ?o2 .
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkWithDoubleGraphPatterns() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?s1 ?o1 ?category
        WHERE {
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :graph mongodb-index:spb500 ;
                   :entity ?entity1 .
          GRAPH mongodb-index:spb500 {
            ?s1 a ?o1 .
          }
          GRAPH mongodb-index:spb500 {
            ?s2 cwork:category ?category .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkDouble_invertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?s1 ?o1 ?category
        WHERE {
          GRAPH mongodb-index:spb100 {
            ?s1 a ?o1 .
          }
          GRAPH mongodb-index:spb100 {
            ?s2 cwork:category ?category .
          }
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :entity ?entity1 .
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkDouble_customInvertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?s1 ?o1 ?category
        WHERE {
          GRAPH mongodb-index:spb500 {
            ?s1 a ?o1 .
          }
          GRAPH mongodb-index:spb500 {
            ?s2 cwork:category ?category .
          }
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :graph mongodb-index:spb500 ;
                   :entity ?entity1 .
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkForDouble_regularAndCustomInvertedGraphs() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?s1 ?o1 ?category
        WHERE {
          GRAPH mongodb-index:spb500 {
            ?s1 a ?o1 .
          }
          GRAPH mongodb-index:spb100 {
            ?s2 cwork:category ?category .
          }
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :graph mongodb-index:spb500 ;
                   :entity ?entity1 .
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void shouldWorkTwoIndexes_doubleCustomInvertedGraphs() throws Exception {

    // the result is graph order dependent.
    // If the graphs or the query blocks below are reordered then the result will be wrong

    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX onto: <http://www.ontotext.com/>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?s1 ?o1 ?s2 ?o2
        WHERE {
          GRAPH mongodb-index:spb500 {
            ?s1 a ?o1 .
          }
          GRAPH mongodb-index:spb600 {
            ?s2 a ?o2
          }
          ?search1 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646461#id'}" ;
                   :graph mongodb-index:spb500 ;
                   :entity ?entity1 .
          ?search2 a mongodb-index:spb100 ;
                   :find "{'@id': 'bbcc:1646453#id'}" ;
                   :graph mongodb-index:spb600 ;
                   :entity ?entity2 .
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void evaluateModelPatternBeforeQuery() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?id ?type ?category ?audience

        WHERE {
          {
            GRAPH mongodb-index:spb100 {
              ?id cwork:category ?category .
              ?id a ?type .
            }
          } UNION {
            GRAPH mongodb-index:spb100 {
              ?id cwork:audience ?audience .
              ?id a ?type .
            }
          }
          ?search a mongodb-index:spb100 ;
                  :find '{"@graph.@type" : "cwork:BlogPost"}' ;
                  :entity ?entity .
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void evaluateModelPatternBeforeQuery_noRdfType() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>

        SELECT ?id ?altText ?category ?audience
        WHERE {
          {
            GRAPH mongodb-index:spb100 {
              ?id cwork:altText ?altText .
              ?id cwork:category ?category .
            }
          } UNION {
            GRAPH mongodb-index:spb100 {
              ?id cwork:altText ?altText .
              ?id cwork:audience ?audience .
            }
          }

          ?search a mongodb-index:spb100 ;
                  :find '{"@graph.@type": "cwork:BlogPost"}' ;
                  :entity ?entity .
        }
        """;
    verifyUnorderedResult();
  }
}
