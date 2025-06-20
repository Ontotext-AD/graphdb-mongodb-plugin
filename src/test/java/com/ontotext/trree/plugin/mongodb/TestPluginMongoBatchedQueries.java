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
    query = "PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>\n"
        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>\n"
        + "PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>\n"
        + "PREFIX atlas: <https://id.roche.com/am/sc/at/>\n"
        + "PREFIX prov: <http://www.w3.org/ns/prov#>\n"
        + "PREFIX dct: <http://purl.org/dc/terms/>\n"
        + "SELECT distinct * WHERE {\n"
        + "    \n"
        + "    {\n"
        + "        [] a mongodb-index:spb100 ;\n"
        + "        mongodb:batchSize 100 ;\n"
        + "        mongodb:find '{}' ;\n"
        + "        mongodb:entity [] .   \n"
        + "\n"
        + "        GRAPH mongodb-index:spb100 {\n"
        + "            ?study skos:prefLabel ?st_label .\n"
        + "            ?study prov:hadPrimarySource ?cr_study .\n"
        + "            ?study ^dct:isPartOf ?act .\n"
        + "        }\n"
        + "    }    \n"
        + "}";

    verifyUnorderedResult();
  }

  /**
   * Test the lazy version of the batch definition.
   */
  @Test
  public void testResultsAreJoined_inverseDef() throws Exception {
    query = "PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>\n"
        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>\n"
        + "PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>\n"
        + "PREFIX atlas: <https://id.roche.com/am/sc/at/>\n"
        + "PREFIX prov: <http://www.w3.org/ns/prov#>\n"
        + "PREFIX dct: <http://purl.org/dc/terms/>\n"
        + "SELECT distinct * WHERE {\n"
        + "    \n"
        + "    {\n"
        + "        GRAPH mongodb-index:spb100 {\n"
        + "            ?study skos:prefLabel ?st_label .\n"
        + "            ?study prov:hadPrimarySource ?cr_study .\n"
        + "            ?study ^dct:isPartOf ?act .\n"
        + "        }\n"
        + "\n"
        + "        [] a mongodb-index:spb100 ;\n"
        + "        mongodb:batchSize 100 ;\n"
        + "        mongodb:find '{}' ;\n"
        + "        mongodb:entity [] .   \n"
        + "    }    \n"
        + "}";

    verifyUnorderedResult();
  }

  @Test
  public void testBadConfig() throws Exception {
    query = "PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>\n"
        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>\n"
        + "PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>\n"
        + "PREFIX atlas: <https://id.roche.com/am/sc/at/>\n"
        + "PREFIX prov: <http://www.w3.org/ns/prov#>\n"
        + "PREFIX dct: <http://purl.org/dc/terms/>\n"
        + "SELECT distinct * WHERE {\n"
        + "    \n"
        + "    {\n"
        + "        [] a mongodb-index:spb100 ;\n"
        + "        mongodb:batchSize \"-1\" ;\n"
        + "        mongodb:find '{}' ;\n"
        + "        mongodb:entity [] .   \n"
        + "\n"
        + "        GRAPH mongodb-index:spb100 {\n"
        + "            ?study skos:prefLabel ?st_label .\n"
        + "            ?study prov:hadPrimarySource ?cr_study .\n"
        + "            ?study ^dct:isPartOf ?act .\n"
        + "        }\n"
        + "" + "    }    \n"
        + "}";

    verifyUnorderedResult();
  }

  @Test
  public void testClientExactCase() throws Exception {
    query = "PREFIX mongodb-index: <http://www.ontotext.com/connectors/mongodb/instance#>\n"
        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "PREFIX mongodb: <http://www.ontotext.com/connectors/mongodb#>\n"
        + "PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>\n"
        + "PREFIX atlas: <https://id.roche.com/am/sc/at/>\n"
        + "PREFIX prov: <http://www.w3.org/ns/prov#>\n"
        + "PREFIX dct: <http://purl.org/dc/terms/>\n"
        + "SELECT distinct * WHERE {\n"
        + "    \n"
        + "    {\n"
        + "        [] a mongodb-index:spb100 ;\n"
        + "        mongodb:batchSize 100 ;\n"
        + "        mongodb:find '{}' ;\n"
        + "        mongodb:entity [] .   \n"
        + "\n"
        + "        GRAPH mongodb-index:spb100 {\n"
        + "            values ?cr_study { <https://id.roche.com/am/a3/jkmxqmxfd296b3k8f6sztmsd67kt> } \n"
        + "            ?study prov:hadPrimarySource ?cr_study .\n"
        + "            ?study ^dct:isPartOf ?act .\n"
        + "        }\n"
        + "    }    \n"
        + "}";

    verifyUnorderedResult();
  }
}
