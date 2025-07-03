package com.ontotext.trree.plugin.mongodb;

import static org.junit.Assert.assertEquals;

import com.mongodb.client.model.Collation;
import com.ontotext.test.utils.StandardUtils;
import com.ontotext.trree.plugin.mongodb.iterator.MongoResultIterator;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoCollationQueries extends AbstractMongoBasicTest {

  @Override
  protected void loadData() {
    loadFilesToMongo();
    addMongoDates();
  }

  @Test
  public void testCreateFirstCollation() {
    MongoResultIterator iterator = new MongoResultIterator(null, null, null, null, null, 1);
    String firstCollation = """
        {
          'locale': 'en',
          'caseLevel': true,
          'caseFirst': 'upper',
          'strength': 1,
          'numericOrdering': true,
          'alternate': 'shifted',
          'maxVariable': 'punct',
          'backwards': true
        }
        """;
    try {
      iterator.setCollation(firstCollation);
      Collation collation = iterator.getCollation();
      assertEquals("en", collation.getLocale());
      assertEquals(true, collation.getCaseLevel());
      assertEquals("upper", collation.getCaseFirst().getValue());
      assertEquals(1, collation.getStrength().getIntRepresentation());
      assertEquals(true, collation.getNumericOrdering());
      assertEquals("shifted", collation.getAlternate().getValue());
      assertEquals("punct", collation.getMaxVariable().getValue());
      assertEquals(true, collation.getBackwards());
    } finally {
      iterator.close();
    }
  }

  @Test
  public void testCreateSecondCollation() {
    MongoResultIterator iterator = new MongoResultIterator(null, null, null, null, null, 1);
    String secondCollation = """
        {
          'locale': 'bg',
          'caseLevel': false,
          'caseFirst': 'lower',
          'strength': 3,
          'numericOrdering': false,
          'alternate': 'non-ignorable',
          'maxVariable': 'space',
          'backwards': false
        }
        """;
    try {
      iterator.setCollation(secondCollation);
      Collation collation = iterator.getCollation();
      assertEquals("bg", collation.getLocale());
      assertEquals(false, collation.getCaseLevel());
      assertEquals("lower", collation.getCaseFirst().getValue());
      assertEquals(3, collation.getStrength().getIntRepresentation());
      assertEquals(false, collation.getNumericOrdering());
      assertEquals("non-ignorable", collation.getAlternate().getValue());
      assertEquals("space", collation.getMaxVariable().getValue());
      assertEquals(false, collation.getBackwards());
    } finally {
      iterator.close();
    }
  }

  @Test
  public void testAggregationWithCollationLowStrength() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          ?search a inst:spb100 ;
                  :aggregate '''[
                    {
                      "$match": {
                        "@graph.cwork:about.@id": "dbpedia:Jaime_Quesada_Chavarría"
                      }
                    }
                  ]''' ;
                  :collation "{'locale': 'en', 'strength': 1}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testAggregationWithCollationDefaultStrength() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          ?search a inst:spb100 ;
                  :aggregate '''[
                    {
                      "$match": {
                        "@graph.cwork:about.@id": "dbpedia:Jaime_Quesada_Chavarría"
                      }
                    }
                  ]''' ;
                  :collation "{'locale': 'en'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultsByFieldWithCollationDefaultStrength() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          BIND(rdf:type AS ?p) .
          ?search a inst:spb100 ;
                  :find '''{'@graph.cwork:about.@id': "dbpedia:Jaime_Quesada_Chavarría"}''' ;
                  :collation "{'locale': 'en'}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultsByFieldWithCollationLowStrength() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          BIND(rdf:type AS ?p) .
          ?search a inst:spb100 ;
                  :find '''{'@graph.cwork:about.@id': "dbpedia:Jaime_Quesada_Chavarría"}''' ;
                  :collation "{'locale': 'en', 'strength': 1}" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }


  @Test
  public void testGetResultsByFieldWithCollationLowStrengthAlternate() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          BIND(rdf:type AS ?p) .
          ?search a inst:spb100 ;
                  :find '''{'@graph.cwork:about.@id': "dbpedia:Jaime_Quesada_Chavarría"}''' ;
                  :collation "{'locale': 'en', 'strength': 1, 'alternate': 'shifted' }" ;
                  :entity ?entity .
          GRAPH inst:spb100 {
            ?s ?p ?o .
          }
        }
        """;
    verifyUnorderedResult();
  }

  @Test
  public void testGetResultsByFieldWithCollationLowStrengthAlternateMaxVariable() throws Exception {
    query = """
        PREFIX : <http://www.ontotext.com/connectors/mongodb#>
        PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>

        SELECT DISTINCT ?entity {
          BIND(rdf:type AS ?p) .
          ?search a inst:spb100 ;
                  :find '''{'@graph.cwork:about.@id': "dbpedia:Jaime_Quesada_Chavarría"}''' ;
                  :collation "{'locale': 'en', 'strength': 1, 'alternate': 'shifted', 'maxVariable': 'space' }" ;
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
