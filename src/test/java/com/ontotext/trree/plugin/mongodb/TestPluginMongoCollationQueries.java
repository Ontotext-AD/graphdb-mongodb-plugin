package com.ontotext.trree.plugin.mongodb;

import static org.junit.Assert.assertEquals;

import com.ontotext.test.utils.StandardUtils;
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
		try {
			String firstCollation = "{'locale': 'en', 'caseLevel': true, 'caseFirst': 'upper'," +
					"'strength': 1, 'numericOrdering': true, 'alternate': 'shifted', 'maxVariable': 'punct', 'backwards': true}";
			iterator.setCollation(firstCollation);
			assertEquals("en", iterator.getCollation().getLocale());
			assertEquals(true, iterator.getCollation().getCaseLevel());
			assertEquals("upper", iterator.getCollation().getCaseFirst().getValue());
			assertEquals(1, iterator.getCollation().getStrength().getIntRepresentation());
			assertEquals(true, iterator.getCollation().getNumericOrdering());
			assertEquals("shifted", iterator.getCollation().getAlternate().getValue());
			assertEquals("punct", iterator.getCollation().getMaxVariable().getValue());
			assertEquals(true, iterator.getCollation().getBackwards());
		} finally {
			iterator.close();
		}
	}

	@Test
	public void testCreateSecondCollation() {
		MongoResultIterator iterator = new MongoResultIterator(null, null, null, null, null, 1);
		try {
			String secondCollation = "{'locale': 'bg', 'caseLevel': false, 'caseFirst': 'lower'," +
					"'strength': 3, 'numericOrdering': false, 'alternate': 'non-ignorable', 'maxVariable': 'space', 'backwards': false}";
			iterator.setCollation(secondCollation);
			assertEquals("bg", iterator.getCollation().getLocale());
			assertEquals(false, iterator.getCollation().getCaseLevel());
			assertEquals("lower", iterator.getCollation().getCaseFirst().getValue());
			assertEquals(3, iterator.getCollation().getStrength().getIntRepresentation());
			assertEquals(false, iterator.getCollation().getNumericOrdering());
			assertEquals("non-ignorable", iterator.getCollation().getAlternate().getValue());
			assertEquals("space", iterator.getCollation().getMaxVariable().getValue());
			assertEquals(false, iterator.getCollation().getBackwards());
		} finally {
			iterator.close();
		}
	}


	@Test
	public void testAggregationWithCollationLowStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "            :aggregate '''[\n"
				+ "{\"$match\": {\"@graph.cwork:about.@id\": \"dbpedia:Jaime_Quesada_Chavarría\"}}\n"
				+ "]''' ;\n"
				+ "\t:collation \"{'locale': 'en', 'strength': 1,  }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testAggregationWithCollationDefaultStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "            :aggregate '''[\n"
				+ "{\"$match\": {\"@graph.cwork:about.@id\": \"dbpedia:Jaime_Quesada_Chavarría\"}}\n"
				+ "]''' ;\n"
				+ "\t:collation \"{'locale': 'en'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetResultsByFieldWithCollationDefaultStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collation \"{'locale': 'en' }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetResultsByFieldWithCollationLowStrength() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collation \"{'locale': 'en','strength': 1}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}


	@Test
	public void testGetResultsByFieldWithCollationLowStrengthAlternate() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collation \"{'locale': 'en','strength': 1, 'alternate': 'shifted' }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Test
	public void testGetResultsByFieldWithCollationLowStrengthAlternateMaxVariable() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select distinct ?entity {\n"
				+ "bind(rdf:type as ?p) . \n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find '''{'@graph.cwork:about.@id' : \"dbpedia:Jaime_Quesada_Chavarría\"}''' ;"
				+ "\t:collation \"{'locale': 'en','strength': 1, 'alternate': 'shifted' , 'maxVariable' : 'space' }\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
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
