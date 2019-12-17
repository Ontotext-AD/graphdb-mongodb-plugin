package com.ontotext.trree.plugin.mongodb;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import com.ontotext.test.utils.StandardUtils;

public class TestPluginMongoGDB4155Exception extends AbstractMongoBasicTest {
	
	@Override
	protected void loadData() {
		loadFilesToMongo();
	}

	@Test
	public void testNoExceptionWithMissingGraph() throws Exception {
		query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
				"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
				"select ?s ?p ?o {\n"
				+ "\t?search a inst:spb100 ;\n"
				+ "#\t:find '{}' ;\n"
				+ ":aggregate '[{ $limit : 10 }]' ;\n"
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
