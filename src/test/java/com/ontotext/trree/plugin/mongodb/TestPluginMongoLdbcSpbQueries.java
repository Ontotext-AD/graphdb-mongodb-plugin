package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class TestPluginMongoLdbcSpbQueries extends AbstractMongoBasicTest {

	protected static File QUERIES_DIR;
	static { 
		try {
			QUERIES_DIR = Paths.get(Thread.currentThread().getContextClassLoader().getResource("mongodb/queries").toURI()).toFile();
		} catch (URISyntaxException e) {
			QUERIES_DIR = Paths.get("src", "test", "resources", "mongodb", "queries").toFile();
		}
	}

	int[] queryIds = new int[] { 1, 2, 3, 4, 5, 7, 9, 12};
	int[] orderedQueries = new int[] {3, 5};

	@Override
	protected void loadData() {
		loadFilesToMongo();
		addMongoDates();
	}

	/**
	 * These are the mongo part of the queries we are currently using in the ldbc spb test (at the moment of writing the
	 * test). They are as complex as it gets so are good candidates for test case. Not all queries are included - only
	 * the ones which are interesting.
	 */
	@Test
	public void testLdbcSpbQueries() throws Exception {
		for (int queryId : queryIds) {
			runQuery(queryId);
		}
	}

	public void runQuery(int num) throws Exception {
		System.out.println("Running query: " + num);
		String query = new String(Files.readAllBytes(new File(QUERIES_DIR, "mongo_query" + num + ".txt").toPath()),
            StandardCharsets.UTF_8);

		File resFile = RESULTS_DIR.resolve(this.getClass().getSimpleName()).resolve("query" + num + ".txt").toFile();
		if (Arrays.binarySearch(orderedQueries, num) > 0) {
			verifyOrderedResult(query, resFile);
		} else {
			verifyUnorderedResult(query, resFile);
		}
	}

	@Override
	protected RepositoryConfig createRepositoryConfiguration() {
		return StandardUtils.createOwlimSe("empty");
	}

}
