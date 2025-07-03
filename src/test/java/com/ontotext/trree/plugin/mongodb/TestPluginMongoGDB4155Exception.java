package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoGDB4155Exception extends AbstractMongoBasicTest {

  @Override
  protected void loadData() {
    loadFilesToMongo();
  }

  @Test
  public void testNoExceptionWithMissingGraph() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?p ?o {
          ?search a inst:spb100 ;
          #        :find '{}' ;
                  :aggregate '[{ $limit : 10 }]' ;
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
