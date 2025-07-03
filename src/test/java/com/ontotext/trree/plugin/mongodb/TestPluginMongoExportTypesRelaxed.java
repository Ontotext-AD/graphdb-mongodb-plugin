package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoExportTypesRelaxed extends AbstractMongoBasicTest {

  @Override
  protected void loadData() {
    loadFilesToMongo();
    addMongoDates();
  }

  @Test
  public void testGetResultFromDocumentById_Long() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?o {
          ?search a inst:spb100 ;
                  :find "{'@id': 'bbcc:1646461#id'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/altText> ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultFromDocumentById_Double() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>
        SELECT ?s ?o {
          ?search a inst:spb100 ;
                  :find "{'@id': 'bbcc:1646461#id'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/altText1> ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultFromDocumentById_Int() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>
        SELECT ?s ?o {
          ?search a inst:spb100 ;
                  :find "{'@id': 'bbcc:1646461#id'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/altText2> ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultFromDocumentById_Array() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT ?s ?o {
          ?search a inst:spb100 ;
                  :find "{'@id': 'bbcc:1646461#id'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s <http://www.bbc.co.uk/ontologies/creativework/altText3> ?o .
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
