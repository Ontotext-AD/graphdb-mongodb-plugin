package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoBatchedQueries extends AbstractMongoBasicTest {

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
  public void testResultsAreJoined() throws Exception {
    query = """
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX atlas: <https://id.roche.com/am/sc/at/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT DISTINCT *
        WHERE {
          {
            [] a mongodb-index:spb100 ;
               mongodb:batchSize 100 ;
               mongodb:find '{}' ;
               mongodb:entity [] .

            GRAPH mongodb-index:spb100 {
              ?study skos:prefLabel ?st_label .
              ?study prov:hadPrimarySource ?cr_study .
              ?study ^dct:isPartOf ?act .
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  /**
   * Test the lazy version of the batch definition.
   */
  @Test
  public void testResultsAreJoined_inverseDef() throws Exception {
    query = """
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX atlas: <https://id.roche.com/am/sc/at/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT DISTINCT *
        WHERE {
          {
            GRAPH mongodb-index:spb100 {
              ?study skos:prefLabel ?st_label .
              ?study prov:hadPrimarySource ?cr_study .
              ?study ^dct:isPartOf ?act .
            }

            [] a mongodb-index:spb100 ;
               mongodb:batchSize 100 ;
               mongodb:find '{}' ;
               mongodb:entity [] .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testBadConfig() throws Exception {
    query = """
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX atlas: <https://id.roche.com/am/sc/at/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT DISTINCT *
        WHERE {
          {
            [] a mongodb-index:spb100 ;
               mongodb:batchSize \"-1\" ;
               mongodb:find '{}' ;
               mongodb:entity [] .

            GRAPH mongodb-index:spb100 {
              ?study skos:prefLabel ?st_label .
              ?study prov:hadPrimarySource ?cr_study .
              ?study ^dct:isPartOf ?act .
            }
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testClientExactCase() throws Exception {
    query = """
        PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX atlas: <https://id.roche.com/am/sc/at/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT DISTINCT *
        WHERE {
          {
            [] a mongodb-index:spb100 ;
               mongodb:batchSize 100 ;
               mongodb:find '{}' ;
               mongodb:entity [] .

            GRAPH mongodb-index:spb100 {
              values ?cr_study {
                <https://id.roche.com/am/a3/jkmxqmxfd296b3k8f6sztmsd67kt>
              }
              ?study prov:hadPrimarySource ?cr_study .
              ?study ^dct:isPartOf ?act .
            }
          }
        }
        """;
    verifyUnorderedResult();
  }
}
