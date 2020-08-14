package com.ontotext.trree.plugin.mongodb;

import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import com.ontotext.test.utils.StandardUtils;

import static org.junit.Assert.assertEquals;

public class TestPluginMongoCollationQueries extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
		addMongoDates();
	}

	@Test
	public void testCreateFirstCollation() {
		MongoResultIterator iterator = new MongoResultIterator(null, null, null, null, null, 1);
		String firstCollation = "{'locale': 'en', 'caseLevel': true, 'caseFirst': 'upper'," +
				"'strength': 1, 'numericOrdering': true, 'alternate': 'shifted', 'maxVariable': 'punct', 'backwards': true}";
		iterator.setCollation(firstCollation);
		assertEquals("en",iterator.collation.getLocale());
		assertEquals(true,iterator.collation.getCaseLevel());
		assertEquals("upper",iterator.collation.getCaseFirst().getValue());
		assertEquals(1,iterator.collation.getStrength().getIntRepresentation());
		assertEquals(true,iterator.collation.getNumericOrdering());
		assertEquals("shifted",iterator.collation.getAlternate().getValue());
		assertEquals("punct",iterator.collation.getMaxVariable().getValue());
		assertEquals(true, iterator.collation.getBackwards());
	}

	@Test
	public void testCreateSecondCollation() {
		MongoResultIterator iterator = new MongoResultIterator(null, null, null, null, null, 1);
		String secondCollation = "{'locale': 'bg', 'caseLevel': false, 'caseFirst': 'lower'," +
				"'strength': 3, 'numericOrdering': false, 'alternate': 'non-ignorable', 'maxVariable': 'space', 'backwards': false}";
		iterator.setCollation(secondCollation);
		assertEquals("bg",iterator.collation.getLocale());
		assertEquals(false,iterator.collation.getCaseLevel());
		assertEquals("lower",iterator.collation.getCaseFirst().getValue());
		assertEquals(3,iterator.collation.getStrength().getIntRepresentation());
		assertEquals(false,iterator.collation.getNumericOrdering());
		assertEquals("non-ignorable",iterator.collation.getAlternate().getValue());
		assertEquals("space",iterator.collation.getMaxVariable().getValue());
		assertEquals(false, iterator.collation.getBackwards());
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
