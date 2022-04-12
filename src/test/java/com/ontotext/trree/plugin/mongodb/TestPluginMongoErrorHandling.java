package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import com.ontotext.trree.sdk.PluginException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestPluginMongoErrorHandling extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
	}

	@Test
	public void testUnrecognizedOrMissingPredicate() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find1 \"{'@id' : 'bbcc:1646461#id'}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .\n"
				+ "\t}\n"
				+ "}";
		expectException(PluginException.class, "Found unrecognized predicate in the MongoDB namespace");

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s <http://www.bbc.co.uk/ontologies/creativework/about> ?o .\n"
				+ "\t}\n"
				+ "}";
		expectException(PluginException.class, "There is no search query for Mongo");
	}

	private void expectException(Class<? extends Exception> exceptionClass, String expectedMessage) {
		Throwable thrown = null;
		try (RepositoryConnection conn = getRepository().getConnection()) {
			try (TupleQueryResult res = conn.prepareTupleQuery(query).evaluate()) {
				if (res.hasNext()) {
					res.next();
				}
			}
		} catch (Exception e) {
			thrown = e;
		}
		if (thrown == null)
			fail("No exception was throws, but one was expected: " + exceptionClass);

		Throwable curr = thrown;
		while (curr != null) {
			if (exceptionClass.isInstance(curr))
				break;
			curr = curr.getCause();
		}
		if (curr == null) {
			fail(String.format("The thrown exception is not of the expected class. Expected: \"%s\", got \"%s\"",
					exceptionClass.getName(), thrown.getClass().getName()));
		}

		if (expectedMessage != null) {
			if (!thrown.getMessage().contains(expectedMessage)) {
				fail(String.format("The thrown exception's message does not contain expected string. Expected: \"%s\", got \"%s\"",
						expectedMessage, thrown.getMessage()));
			}
		}
	}
	@Override
	protected RepositoryConfig createRepositoryConfiguration() {
		return StandardUtils.createOwlimSe("empty");
	}

}
