package com.ontotext.trree.plugin.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.ontotext.trree.sdk.*;
import com.ontotext.trree.sdk.Entities.Scope;
import org.apache.commons.io.FileUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class MongoDBPlugin extends PluginBase implements SystemPlugin, Preprocessor, PatternInterpreter, UpdateInterpreter, PluginTransactionListener {

	public static final String NAMESPACE = "http://www.ontotext.com/connectors/mongodb#";
	public static final String NAMESPACE_INST = "http://www.ontotext.com/connectors/mongodb/instance#";

	//update predicates
	public static final IRI SERVICE = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "service");
	public static final IRI DATABASE = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "database");
	public static final IRI COLLECTION = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "collection");
	public static final IRI USER = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "user");
	public static final IRI PASSWORD = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "password");
	public static final IRI AUTH_DB = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "authDb");
	public static final IRI DROP = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "drop");
	// query predicates
	public static final IRI QUERY = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "find");
	public static final IRI PROJECTION = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "project");
	public static final IRI AGGREGATION = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "aggregate");
	public static final IRI HINT = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "hint");
	public static final IRI ENTITY = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "entity");
	public static final IRI GRAPH = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "graph");

	protected static final String MONGODB_PROPERTIES = "mongodb.properties";

	protected ValueFactory vf = SimpleValueFactory.getInstance();

	// predicateIds
	long serviceId = 0, databaseId = 0, collectionId = 0, userId = 0, passwordId = 0, authDbId = 0, dropId = 0;

	long queryId = 0;
	long projectionId = 0;
	long aggregationId = 0;
	long hintId = 0;
	long entityId = 0;
	long graphId = 0;
	long rdf_type = 0;

	protected long[] predicateSet;

	protected Map<String, Configuration> configMap = new HashMap<>();
	protected Map<String, MongoClient> mongoClients = new HashMap<>();

	/**
	 * this is the context implementation where the plugin stores currently running patterns
	 * it just keeps some values using sting keys for further access
	 */
	class ContextImpl implements RequestContext {
		RequestCache cache = new RequestCache();
		HashMap<String, Object> map = new HashMap<String, Object>();
		Request request;
		LinkedList<MongoResultIterator> iters;
		Set<Long> contexts = new HashSet<>();
		Entities entities;
		long searchBNode;

		@Override
		public Request getRequest() {
			return request;
		}

		@Override
		public void setRequest(Request request) {
			this.request = request;
		}

		public Object getAttribute(String key) {
			return map.get(key);
		}

		public void setAttribute(String key, Object value) {
			map.put(key, value);
		}

		public void removeAttribute(String key) {
			map.remove(key);
		}

		public void addIterator(MongoResultIterator iter) {
			if (this.iters == null)
				this.iters = new LinkedList<>();
			this.iters.add(iter);
		}

		public void addContext(long ctx) {
			contexts.add(ctx);
		}

		public Set<Long> getContexts() {
			return contexts;
		}
	}

	@Override
	public String getName() {
		return "mongodb";
	}

	/**
	 * init the plugin
	 */
	@Override
	public void initialize(InitReason initReason, PluginConnection pluginConnection) {
		Entities entities = pluginConnection.getEntities();
		// register the predicates
		serviceId = entities.put(SERVICE, Scope.SYSTEM);
		databaseId = entities.put(DATABASE, Scope.SYSTEM);
		collectionId = entities.put(COLLECTION, Scope.SYSTEM);
		userId = entities.put(USER, Scope.SYSTEM);
		passwordId = entities.put(PASSWORD, Scope.SYSTEM);
		authDbId = entities.put(AUTH_DB, Scope.SYSTEM);
		dropId = entities.put(DROP, Scope.SYSTEM);

		queryId = entities.put(QUERY, Scope.SYSTEM);
		projectionId = entities.put(PROJECTION, Scope.SYSTEM);
		aggregationId = entities.put(AGGREGATION, Scope.SYSTEM);
		hintId = entities.put(HINT, Scope.SYSTEM);
		entityId = entities.put(ENTITY, Scope.SYSTEM);
		graphId = entities.put(GRAPH, Scope.SYSTEM);
		rdf_type = entities.resolve(RDF.TYPE);

		predicateSet = new long[] {serviceId, databaseId, collectionId, userId, passwordId, authDbId, dropId, queryId,
				projectionId, aggregationId, hintId, entityId, graphId, rdf_type};
		Arrays.sort(predicateSet);
	}

	@Override
	public double estimate(long subject, long predicate, long object, long context, PluginConnection pluginConnection,
						   RequestContext requestContext) {
		ContextImpl ctx = (requestContext instanceof ContextImpl) ? (ContextImpl) requestContext : null;
		if (predicate == rdf_type) {
			if (ctx != null && ctx.iters != null && object != 0) {
				if (object == Entities.BOUND)
					return 0.3;
				String suffix = Utils.matchPrefix(pluginConnection.getEntities().get(object).stringValue(), NAMESPACE_INST);
				if (suffix != null && suffix.length() > 0) {
					return 0.3;
				}
			}
		}
		if (predicate == graphId) {
			return 0.35;
		}
		if (predicate == aggregationId) {
			return 0.39;
		}
		if (predicate == queryId) {
			return 0.43;
		}
		if (predicate == projectionId) {
			return 0.46;
		}
		if (predicate == hintId) {
			return 0.49;
		}
		if (predicate == entityId) {
			return 0.52;
		}
		if (ctx != null && ctx.iters != null && ctx.getContexts().contains(context)) {
			return 0.6;
		}
		return 0;
	}

	@Override
	public StatementIterator interpret(long subject, long predicate, long object, long context,
                                       PluginConnection pluginConnection, RequestContext requestContext) {
		// make sure we have the proper request context set when preprocess() has been invoked
		// if not return EMPTY
		ContextImpl ctx = (requestContext instanceof ContextImpl) ? (ContextImpl) requestContext : null;

		// not our context
		if (ctx == null)
			return StatementIterator.EMPTY;

		Entities entities = pluginConnection.getEntities();

		if (predicate != 0 && context == 0) {
			Value val = entities.get(predicate);

			if (val != null && val instanceof IRI) {
				String ns = ((IRI) val).getNamespace();

				if (NAMESPACE.equals(ns) || NAMESPACE_INST.equals(ns)) {
					if (Arrays.binarySearch(predicateSet, predicate) < 0) {
						throw new PluginException("Found unrecognized predicate in the MongoDB namespace: " + val.stringValue());
					}
				}
			}
		}
		if (ctx.entities == null) {
			ctx.entities = entities;
		}
		if (rdf_type == 0) {
			rdf_type = entities.resolve(RDF.TYPE);
			if (rdf_type == 0)
				return null;
		}
		if (predicate == rdf_type && context == 0) {
			if (object >= 0)
				return null;
			if (subject != 0)
				return null;
			String suffix = Utils.matchPrefix(Utils.getString(entities, object), NAMESPACE_INST);
			if (suffix == null)
				return null;
			if (ctx.iters != null && subject != 0) {
				for (MongoResultIterator it : ctx.iters) {
					if (it.searchSubject == subject)
						return it;
				}
			}

			Configuration config = configMap.computeIfAbsent(suffix, s -> {
				File indexInst = new File(getDataDir(), suffix);
				if (!indexInst.exists()) {
					getLogger().error("MongoDB service {} not connected!", suffix);
					return null;
				}
				File propertiesFile = new File(indexInst, MONGODB_PROPERTIES);
				try {
					return Configuration.fromFile(propertiesFile);
				} catch (IOException e) {
					getLogger().error("Cannot get MongoDB connect parameters {}. Exception {}!", suffix, e);
					return null;
				}
			});

			if (config == null)
				return StatementIterator.EMPTY;

			String connectString = config.getConnectionString();
			String database = config.getDatabase();
			String collection = config.getCollection();
			Optional<MongoCredential> mongoCredential = config.getMongoCredential();

			if (connectString == null || connectString.trim().length() == 0) {
				getLogger().error("Invalid connect parameters for MongoDB inst {}!", suffix);
				return StatementIterator.EMPTY;
			}
			// get
			long id = entities.put(vf.createBNode(), Scope.REQUEST);
			ctx.searchBNode = id;
			ctx.addContext(object);
			return createMainIterator(connectString, database, collection, mongoCredential, ctx);
		}

		if (predicate == graphId) {

			if (ctx.iters == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);

			if (object != 0) {
				iter.setGraphId(object);
				ctx.addContext(object);
			}

			return iter.singletonIterator(queryId, object);
		}
		if (predicate == queryId) {
			String queryString = Utils.getString(entities, object);

			if (ctx.iters == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setQuery(queryString);
			return iter.singletonIterator(queryId, object);
		}
		if (predicate == projectionId) {
			String projectionString = Utils.getString(entities, object);

			if (ctx.iters == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}
			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setProjection(projectionString);
			return iter.singletonIterator(projectionId, object);
		}
		if (predicate == aggregationId) {
			String aggregationString = Utils.getString(entities, object);

			if (aggregationString == null) {
				return StatementIterator.EMPTY;
			}
			if (ctx.iters == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			List<Document> aggregation = new LinkedList<>();
			try {
				for (BsonValue doc : BsonArray.parse(aggregationString)) {
					aggregation.add(Document.parse(((BsonDocument) doc).toJson()));
				}
			} catch (Exception e) {
				getLogger().error("could not parse aggregation parameter", e);
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setAggregation(aggregation);
			return iter.singletonIterator(aggregationId, object);
		}
		if (predicate == hintId) {
			String hintString = Utils.getString(entities, object);

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setHint(hintString);
			return iter.singletonIterator(hintId, object);
		}
		if (predicate == entityId) {
			if (ctx.iters == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);
			return iter.createEntityIter(entityId);
		}

		if (context != 0) {
			if (ctx.iters == null) {
				String suffix = Utils.matchPrefix(entities.get(context).stringValue(), NAMESPACE_INST);
				if (suffix != null && suffix.length() > 0) {
					File indexInst = new File(getDataDir(), suffix);
					if (!indexInst.exists()) {
						getLogger().error("MongoDB service {} not connected!", suffix);
						return StatementIterator.EMPTY;
					}
					Configuration config = configMap.get(suffix);
					if (config == null) {
						getLogger().error("Cannot get MongoDB connect parameters {}!", suffix);
						return StatementIterator.EMPTY;
					}
					String connectString = config.getConnectionString();
					String database = config.getDatabase();
					String collection = config.getCollection();
					Optional<MongoCredential> mongoCredential = config.getMongoCredential();

					if (connectString == null || connectString.trim().length() == 0) {
						getLogger().error("Invalid connect paramters for MongoDB inst {}!", suffix);
						return StatementIterator.EMPTY;
					}
					// get
					long id = entities.put(vf.createBNode(), Scope.REQUEST);
					ctx.searchBNode = id;
					ctx.addContext(context);

					ctx.iters.add((MongoResultIterator) createMainIterator(connectString, database, collection, mongoCredential, ctx));
				}

			}
			if (ctx.iters != null) {
				return getIterator(subject, context, ctx).getModelIterator(subject, predicate, object);
			}
		}
		return null;
	}

	@Override
	public RequestContext preprocess(Request request) {
		ContextImpl impl = new ContextImpl();
		impl.setRequest(request);
		return impl;
	}

	// UpdateInterpreter related code
	// The plugin listen for 
	@Override
	public long[] getPredicatesToListenFor() {
		return new long[]{serviceId, databaseId, collectionId, userId, authDbId, passwordId, dropId};
	}

	@Override
	public boolean interpretUpdate(long subject, long predicate, long object, long context, boolean isAddition,
								   boolean isExplicit, PluginConnection pluginConnection) {
		// skip deletions
		if (!isAddition) {
			return false;
		}
		if (predicate == serviceId || predicate == databaseId || predicate == collectionId || predicate == userId
				|| predicate == passwordId || predicate == authDbId) {
			// no valid object or subject
			// get new instance localname
			String suffix = Utils.matchPrefix(Utils.getString(pluginConnection.getEntities(), subject), NAMESPACE_INST);
			if (suffix == null) {
				getLogger().error("No valid localname for the instance when registering a connection to MongoDB");
				return true;
			}
			String value = Utils.getString(pluginConnection.getEntities(), object);

			Configuration config = configMap.computeIfAbsent(suffix, s -> {
				File indexFolder = new File(getDataDir(), suffix);
				if (!indexFolder.exists()) {
					getLogger().info("Creating a new service in MongoDB: {}", s);
					indexFolder.mkdirs();
				}
				return new Configuration(new File(indexFolder, MONGODB_PROPERTIES));
			});

			if (predicate == serviceId) {
				config.setConnectionString(value);
				logUpdatedSetting(suffix, Configuration.CONNECTION_STRING_PROPERTY);
			} else if (predicate == databaseId) {
				config.setDatabase(value);
				logUpdatedSetting(suffix, Configuration.DATABASE_PROPERTY);
			} else if (predicate == collectionId) {
				config.setCollection(value);
				logUpdatedSetting(suffix, Configuration.COLLECTION_PROPERTY);
			} else if (predicate == userId) {
				config.setUser(value);
				logUpdatedSetting(suffix, Configuration.USER_PROPERTY);
			} else if (predicate == authDbId) {
				config.setAuthDb(value);
				logUpdatedSetting(suffix, Configuration.AUTH_DB_PROPERTY);
			} else if (predicate == passwordId) {
				config.setPassword(value);
				logUpdatedSetting(suffix, Configuration.PASSWORD_PROPERTY);
			}

			return true;
		}
		if (predicate == dropId) {
			String suffix = Utils.matchPrefix(Utils.getString(pluginConnection.getEntities(), subject), NAMESPACE_INST);
			if (suffix == null) {
				getLogger().error("No valid localname for the instance when registering a connection to MongoDB");
				return true;
			}

			Configuration conf = configMap.remove(suffix);
			if (conf != null) {
				conf.delete();
			}
			File indexFolder = new File(getDataDir(), suffix);
			if (indexFolder.exists()) {
				try {
					FileUtils.deleteDirectory(indexFolder);
					getLogger().info("MongoDB service {} removed successfully", suffix);
				} catch (IOException e) {
					getLogger().error("Cannot remove folder {} for MongoDB service {}!", indexFolder.getAbsolutePath(), suffix);
				}
			} else {
				getLogger().warn("Could not remove MongoDB service {} because it was not registered!", suffix);
			}
			return true;
		}
		return false;
	}

	protected MongoResultIterator getIterator(long subject, long context, ContextImpl ctx) {
		if (subject != 0) {
			for (MongoResultIterator it : ctx.iters) {
				if (it.searchSubject == subject)
					return it;
			}
		}
		if (context != 0) {
			Iterator<MongoResultIterator> iter = ctx.iters.descendingIterator();
			while (iter.hasNext()) {
				MongoResultIterator curr = iter.next();
				if (curr.graphId == context) {
					return curr;
				}
			}
		}
		return ctx.iters.getLast();
	}

	protected StatementIterator createMainIterator(String connect, String database, String collection, Optional<MongoCredential> mongoCredential, ContextImpl ctx) {
		MongoClient client = mongoClients.computeIfAbsent(
				connect + (mongoCredential.isPresent() ? mongoCredential.get().getUserName() + "_" + mongoCredential.get().getSource(): ""), s -> {
			MongoClientSettings.Builder builder = MongoClientSettings.builder();

			builder.applyConnectionString(new ConnectionString(connect));
			if (mongoCredential.isPresent()) {
				builder.credential(mongoCredential.get());
			}

			return MongoClients.create(builder.build());
		});


		MongoResultIterator ret = new MongoResultIterator(this, client, database, collection, ctx.cache, ctx.searchBNode);
		ret.entities = ctx.entities;
		ctx.addIterator(ret);
		return ret;
	}

	@Override
	public void transactionStarted(PluginConnection pluginConnection) {
	}

	@Override
	public void transactionCompleted(PluginConnection pluginConnection) {
	}

	@Override
	public void transactionAborted(PluginConnection pluginConnection) {
	}

	@Override
	public void transactionCommit(PluginConnection pluginConnection) {
		for (Map.Entry<String, Configuration> entry : configMap.entrySet()) {
			if (entry.getValue().isDirty()) {
				try {
					entry.getValue().persist();
				} catch (IOException e) {
					getLogger().error("cannot register mongoDB connection into {} for index {}. IOException {}", entry.getValue().getPropertiesFilePath(), entry.getKey(), e);
					throw new PluginException("Could not persist mongo configuration: " + e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public void shutdown(ShutdownReason shutdownReason) {
		for (MongoClient client : mongoClients.values()) {
			client.close();
		}
	}

	private void logUpdatedSetting(String suffix, String setting) {
		getLogger().info("Setting {} for MongoDB service {}", setting, suffix);
	}
}
