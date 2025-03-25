package com.ontotext.trree.plugin.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.ontotext.graphdb.Config;
import com.ontotext.test.TemporaryLocalFolder;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.QueryResult;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Convenient test case for the scenario: upload date to mongo, query it, verify the result.
 * If you are using the verifyOrderedResult() method you can use the LEARN_MODE option which will generate the
 * output files for you.
 */
public abstract class AbstractMongoBasicTest extends AbstractMongoTest {

	protected static final String CONNECT_MONGODB_LOCALHOST = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
			"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
			"insert data {\n"
			+ "inst:spb100 :service \"%s\" ."
			+ "inst:spb100 :database \"%s\" ."
			+ "inst:spb100 :collection \"%s\" ."
			+ "}";

	protected static final String DROP_MONGODB_LOCALHOST = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\r\n" +
			"PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\r\n" +
			"insert data {\n"
			+ "inst:spb100 :drop \"%s\" ."
			+ "}";


	protected static Path INPUT_DIR;
	static {
		try {
			INPUT_DIR = Paths.get(Thread.currentThread().getContextClassLoader().getResource("mongodb/input").toURI());
		} catch (URISyntaxException e) {
			INPUT_DIR = Paths.get("src", "test", "resources", "mongodb", "input");
		}
	}
	protected static Path RESULTS_DIR;
	static {
		try {
			RESULTS_DIR = Paths.get(Thread.currentThread().getContextClassLoader().getResource("mongodb/results").toURI());
		} catch (URISyntaxException e) {
			RESULTS_DIR = Paths.get("src", "test", "resources", "mongodb", "results");
		}
	}

	protected static final String MONGODB_DATABASE = "ldbc";
	protected static final String MONGODB_COLLECTION = "creativeWorks";

	protected MongoClient mongo;
	protected MongoCollection<Document> collection;

	// Used when instrumenting the dates in Mongo to be in ISODate format.
	protected String dateStringFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
	protected DateFormat dateFormat;

	protected String query;

	@ClassRule
	public static TemporaryLocalFolder tmp = new TemporaryLocalFolder();

	public AbstractMongoBasicTest() {
		dateFormat = new SimpleDateFormat(dateStringFormat);
	}

	protected abstract void loadData();

	@BeforeClass
	public static void setWorkDir() {
		System.setProperty("graphdb.home.work", String.valueOf(tmp.getRoot()));
		Config.reset();
	}

	@AfterClass
	public static void resetWorkDir() {
		System.clearProperty("graphdb.home.work");
		Config.reset();
	}

	@Before
	public void setup() {
		super.setup();

		connectToMongoDB();
		loadData();
	}

	@After
	public void cleanup() {
		disconnectFromMongoDB();
		collection.drop();
		mongo.close();

		super.cleanup();
	}

	/**
	 * Executes a query and verifies the result count is correct
	 *
	 * @param query								query to be executed
	 * @param expectedResultsCount
	 */
	protected void verifyResultsCount(String query, int expectedResultsCount) {
		try (RepositoryConnection conn = getRepository().getConnection()) {

			TupleQueryResult iter = conn.prepareTupleQuery(query).evaluate();

			int countResults = 0;
			while (iter.hasNext()) {
				iter.next();
				countResults++;
			}

			assertEquals("Iterator should return four results but were " + countResults, expectedResultsCount, countResults);
		}
	}

	protected void verifyOrderedResult() throws Exception {
		verifyResult(query, RESULTS_DIR.resolve(this.getClass().getSimpleName()).resolve(Thread.currentThread().getStackTrace()[2].getMethodName()).toFile(), true);
	}

	protected void verifyUnorderedResult() throws Exception {
		verifyResult(query, RESULTS_DIR.resolve(this.getClass().getSimpleName()).resolve(Thread.currentThread().getStackTrace()[2].getMethodName()).toFile(), false);
	}

	/**
	 * Executes a query verifies the output matches exactly a given one. Depending on the LEARN_MODE setting this method
	 * will wither compare the result of the query or generate new output files. <p>
	 * As the comparison is exact one make sure you do not have any blank nodes in the result!
	 *
	 * @param query			 query to be executed
	 * @param resultFile file	containing the output to be compared against.
	 */
	protected void verifyOrderedResult(String query, File resultFile) throws Exception {
		verifyResult(query, resultFile, true);
	}

	/**
	 * Executes a query verifies the output matches a given one no matter the order in which the bindings come. Depending
	 * on the LEARN_MODE setting this method will wither compare the result of the query or generate new output files. <p>
	 *
	 * @param query			 query to be executed
	 * @param resultFile file containing the output to be compared against.
	 */
	protected void verifyUnorderedResult(String query, File resultFile) throws Exception {
		verifyResult(query, resultFile, false);
	}

	protected void verifyResult(String resultFile, boolean ordered) throws Exception {
		verifyResult(query, RESULTS_DIR.resolve(this.getClass().getSimpleName()).resolve(resultFile).toFile(), ordered);
	}

	protected void verifyResult(String query, File resultFile, boolean ordered) throws Exception {
		try (RepositoryConnection conn = getRepository().getConnection()) {

			QueryResult iter;

			if (conn.prepareQuery(query) instanceof GraphQuery) {
				iter = conn.prepareGraphQuery(query).evaluate();
			} else {
				iter = conn.prepareTupleQuery(query).evaluate();
			}

			File actualFile = tmpFolder.newFile(resultFile.getName() + "_actual");
			File writeTo = isLearnMode() ? resultFile : actualFile;
			if (isLearnMode() && !writeTo.exists()) {
				writeTo.getParentFile().mkdirs();
				writeTo.createNewFile();
			}
			try (OutputStream os = new FileOutputStream(writeTo)) {
				while (iter.hasNext()) {
					String bs = iter.next().toString()
							.replace("^^<http://www.w3.org/2001/XMLSchema#string>", "");
					os.write(bs.getBytes(StandardCharsets.UTF_8));
					os.write("\n".getBytes(StandardCharsets.UTF_8));
					System.out.println(bs);
				}
			}

			if (!isLearnMode()) {
				List<String> exp = Files.readAllLines(resultFile.toPath(), StandardCharsets.UTF_8);
				List<String> act = Files.readAllLines(actualFile.toPath(), StandardCharsets.UTF_8);

				if (!ordered) {
					exp.sort(String::compareTo);
					act.sort(String::compareTo);
				}

				assertEquals("Number of results", exp.size(), act.size());

				for (int i = 0; i < act.size(); i++) {
					assertEquals("Result record is as expected", exp.get(i).replace("[null]", "").trim(), act.get(i).replace("[null]", "").trim());
				}
			}
		}
	}

	protected void loadFilesToMongo() {
		loadFilesToMongo(INPUT_DIR.resolve(this.getClass().getSimpleName()).toFile());
	}

	/**
	 * Load the content of a given folder to mongo
	 *
	 * @param inputFolder
	 */
	protected void loadFilesToMongo(File inputFolder) {
		List<Document> batch = new LinkedList<Document>();

		for (File file : inputFolder.listFiles()) {
			if (file.isDirectory()) {
				continue;
			}
			try {
				String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
				if (content.startsWith("{")) {
					batch.add(Document.parse(content));
				} else if (content.startsWith("[")) {
					for (BsonValue bsonValue : BsonArray.parse(content)) {
						batch.add(Document.parse(bsonValue.asString().getValue()));
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		collection.insertMany(batch);
	}

	/**
	 * Determines whether the methods which compare query results against given output generate the output or do the
	 * actual comparing<p>
	 *		 When running the actual tests this should always return false and can be set to true only when writing new
	 *		 test cases.
	 * @return
	 */
	protected boolean isLearnMode() {
		return false;
	}

	protected void addMongoDates() {
		FindIterable<Document> documents = collection.find();

		try {
			for (Document doc : documents) {
				ObjectId id = doc.getObjectId("_id");
				List<Map<String, Object>> graph = (List<Map<String, Object>>) doc.get("@graph");

				for (Map.Entry currentValue : graph.get(0).entrySet()) {
					Object object = graph.get(0).get(currentValue.getKey());

					if (object instanceof Document) {
						addMongoDates((Document) object, id, currentValue.getKey().toString());
					} else if(object instanceof ArrayList) {
						addMongoDates((ArrayList<Object>) object, id, currentValue.getKey().toString());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void addMongoDates(ArrayList<Object> objects, ObjectId objectId, String prefix) throws ParseException {
		for (Object object : objects) {
			if (object instanceof Document) {
				for (Map.Entry currentValue : ((Document) object).entrySet()) {
					StringBuilder sb = new StringBuilder();
					sb.append(prefix);
					if (currentValue.getValue() instanceof Document) {
						sb.append("[").append(currentValue.getKey().toString());
						addMongoDates((Document) currentValue.getValue(), objectId, sb.toString());
					} else if (currentValue.getValue() instanceof ArrayList) {
						sb.append("[" + currentValue.getKey().toString());
						addMongoDates((ArrayList<Object>) currentValue.getValue(), objectId, sb.toString());
					}
				}
			}
		}
	}

	private void addMongoDates(Document doc, ObjectId objectId, String prefix) throws ParseException {
		if (doc.get("@type") != null && doc.get("@type").equals("xsd:dateTime")) {
			StringBuilder sb = new StringBuilder();
			sb.append("@graph.0.");
			int index = prefix.indexOf("[");
			if (index == -1) {
				sb.append(prefix).append(".@date");
			} else {
				sb.append(prefix).append(".@date");
				appendBrackets(sb);
			}
			Bson newValue = new Document(sb.toString(), dateFormat.parse(doc.get("@value").toString()));
			Bson updateNewValue = new Document("$set", newValue);
			collection.updateOne(new Document("_id", objectId), updateNewValue);
		}
	}

	private void appendBrackets(StringBuilder sb) {
		int index = sb.indexOf("[");

		while (index != -1) {
			sb.append("]");
			index = sb.indexOf("[", index + 1);
		}
	}

	protected void connectToMongoDB() {
		try (RepositoryConnection conn = getRepository().getConnection()) {
			conn.prepareUpdate(String.format(CONNECT_MONGODB_LOCALHOST, getServiceName(), MONGODB_DATABASE, MONGODB_COLLECTION)).execute();

			mongo = MongoClients.create(getServiceName());
			collection = mongo.getDatabase(MONGODB_DATABASE).getCollection(MONGODB_COLLECTION);
		}
	}

	protected void disconnectFromMongoDB() {
		try (RepositoryConnection conn = getRepository().getConnection()) {
			conn.prepareUpdate(String.format(DROP_MONGODB_LOCALHOST, getServiceName())).execute();
		}
	}

	protected String getServiceName() {
		return "mongodb://localhost:" + mongoProcess.getConfig().net().getPort();
	}

}
