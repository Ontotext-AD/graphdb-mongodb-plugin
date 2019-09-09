package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

public class TestPluginMongoKeysEscaping extends AbstractMongoBasicTest {

	@Override
	protected void loadData() {
		loadFilesToMongo();
	}

	/**
	 * Mongo does not support "." and "$" (as first char) in the keys of the documents. We support encoding the keys
	 * so we can have full URIs as keys and predicates starting with $ (for whatever reason we would want that). In the
	 * JSON-LDs only the keys should be decoded and the values should be returned as they are in the mongo document.
	 */
	@Test
	public void testOnlyKeysAreDecoded() throws Exception {

		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?p ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "\t:find \"{}\" ;"
				+ "\t:entity ?entity .\n"
				+ "\tgraph inst:spb100 {\n"
				+ "\t\t?s ?p ?o .\n"
				+ "\t}\n"
				+ "}";

		verifyUnorderedResult();
	}

	@Override
	protected RepositoryConfig createRepositoryConfiguration() {
		return StandardUtils.createOwlimSe("empty");
	}

}
