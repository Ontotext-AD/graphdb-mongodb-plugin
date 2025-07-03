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
   * node in the beginning.
   */
  @Test
  public void testJsonLdWithAndWithoutContexts() {
    String query = """
          PREFIX : <http://www.ontotext.com/connectors/mongodb#>
          PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

          SELECT distinct ?entity {
            ?search a inst:spb100 ;
                    :find "{}" ;
                    :entity ?entity .
            GRAPH inst:spb100 {
              ?s ?p ?o .
            }
          }
        """;

    verifyResultsCount(query, 2);
  }

  /**
   * Custom fields can be added to the mongo doc as nodes in the "custom" node. They should be parsed separately as they
   * are not part of the json-ld standard.
   */
  @Test
  public void testCustomFields() throws Exception {
    query = """
          PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>
          PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>
          PREFIX : <http://www.ontotext.com/connectors/mongodb#>

          SELECT ?p ?o {
            ?search a inst:spb100 ;
                    :aggregate '''[
                      {
                        "$match": {
                          "@graph.@type": "cwork:NewsItem"
                        }
                      },
                      {
                        "$count": "size"
                      },
                      {
                        "$project": {
                          "custom.size": "$size",
                          "custom.halfSize": {
                            "$divide": ["$size", 2]
                          }
                        }
                      }]''' ;
                    :entity ?entity .
            GRAPH inst:spb100 {
              ?s ?p ?o .
            }
          }
        """;
    verifyUnorderedResult();
  }

  @Override
  protected RepositoryConfig createRepositoryConfiguration() {
    return StandardUtils.createOwlimSe("empty");
  }
}
