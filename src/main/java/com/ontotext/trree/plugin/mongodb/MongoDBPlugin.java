package com.ontotext.trree.plugin.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.ontotext.trree.plugin.mongodb.configuration.Configuration;
import com.ontotext.trree.plugin.mongodb.iterator.LazyMongoResultIterator;
import com.ontotext.trree.plugin.mongodb.iterator.MongoResultIterator;
import com.ontotext.trree.plugin.mongodb.request.ContextPhase;
import com.ontotext.trree.plugin.mongodb.request.PluginRequestContext;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.Entities.Scope;
import com.ontotext.trree.sdk.InitReason;
import com.ontotext.trree.sdk.PatternInterpreter;
import com.ontotext.trree.sdk.PluginBase;
import com.ontotext.trree.sdk.PluginConnection;
import com.ontotext.trree.sdk.PluginException;
import com.ontotext.trree.sdk.PluginTransactionListener;
import com.ontotext.trree.sdk.Preprocessor;
import com.ontotext.trree.sdk.Request;
import com.ontotext.trree.sdk.RequestContext;
import com.ontotext.trree.sdk.ShutdownReason;
import com.ontotext.trree.sdk.StatementIterator;
import com.ontotext.trree.sdk.UpdateInterpreter;
import com.ontotext.trree.sdk.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class MongoDBPlugin extends PluginBase implements Preprocessor, PatternInterpreter, UpdateInterpreter, PluginTransactionListener {

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
	public static final IRI COLLATION = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "collation");
	public static final IRI BATCH = SimpleValueFactory.getInstance().createIRI(NAMESPACE + "batchSize");

	protected static final String MONGODB_PROPERTIES = "mongodb.properties";

	public final int MAX_BATCH_SIZE;

	{
		int maxBatch;
		try {
			maxBatch = Integer.parseInt(System.getProperty("graphdb.mongodb.maxBatchSize", "1000"));
		} catch (NumberFormatException e) {
			getLogger().error("Invalid graphdb.mongodb.maxBatchSize: {}", System.getProperty(
							"graphdb.mongodb.maxBatchSize"));
			maxBatch = 1000;
		}
		if (maxBatch > 10000) {
			getLogger().warn("graphdb.mongodb.maxBatchSize size is too large. Max allowed is 10000");
			maxBatch = 10000;
		}
		if (maxBatch == 0) {
			getLogger().info("MongoDB batch loading is disabled");
		}
		MAX_BATCH_SIZE = maxBatch;
	}

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

	// predicateIds
	long serviceId = 0, databaseId = 0, collectionId = 0, userId = 0, passwordId = 0, authDbId = 0, dropId = 0;

	long queryId = 0;
	long projectionId = 0;
	long aggregationId = 0;
	long hintId = 0;
	long entityId = 0;
	long graphId = 0;
	long rdf_type = 0;
	long collationId = 0;
	long batchSize = 0;

	protected long[] predicateSet;

	protected Map<String, Configuration> configMap = new HashMap<>();
	protected Map<String, MongoClient> mongoClients = new HashMap<>();

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
		collationId = entities.put(COLLATION, Scope.SYSTEM);
		batchSize = entities.put(BATCH, Scope.SYSTEM);


		predicateSet = new long[] {serviceId, databaseId, collectionId, userId, passwordId, authDbId, dropId, queryId,
				projectionId, aggregationId, hintId, entityId, graphId, collationId, batchSize, rdf_type};
		Arrays.sort(predicateSet);
	}

	@Override
	public double estimate(long subject, long predicate, long object, long context, PluginConnection pluginConnection,
							 RequestContext requestContext) {

      PluginRequestContext ctx =
          (requestContext instanceof PluginRequestContext) ? (PluginRequestContext) requestContext : null;
		if (predicate == rdf_type) {
        if (ctx != null && ctx.getIterators() != null && object != 0 && object != Entities.BOUND) {
				String suffix = Utils.matchPrefix(
								pluginConnection.getEntities().get(object).stringValue(), NAMESPACE_INST);
				if (suffix != null && suffix.length() > 0) {
					return 0.3;
				}
			}
		}
		if (predicate == graphId) {
			return 0.35;
		}
		if (predicate == batchSize) {
			return 0.37;
		}
		if (predicate == aggregationId) {
			return 0.39;
		}
		if (predicate == queryId) {
			return 0.43;
		}
		if (predicate == collationId) {
			return 0.45;
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
        if (ctx != null && ctx.getIterators() != null && ctx.getContexts().contains(context)) {
			return 0.6;
		}
		return 0;
	}

	@Override
	public StatementIterator interpret(long subject, long predicate, long object, long context,
																			 PluginConnection pluginConnection, RequestContext requestContext) {
		// make sure we have the proper request context set when preprocess() has been invoked
		// if not return EMPTY
        PluginRequestContext ctx =
            (requestContext instanceof PluginRequestContext) ? (PluginRequestContext) requestContext : null;

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
        if (ctx.getEntities() == null) {
          ctx.setEntities(entities);
		}
		if (rdf_type == 0) {
			rdf_type = entities.resolve(RDF.TYPE);
			if (rdf_type == 0)
				return null;
		}
		if (predicate == rdf_type && context == 0) {
			if (object >= 0)
				return null;
			String suffix = Utils.matchPrefix(Utils.getString(entities, object), NAMESPACE_INST);
			if (suffix == null)
				return null;
              if (ctx.getIterators() != null) {
				// we can have an iterator that is created by a graph pattern put before the query itself
				// this way we can match the graph id of that iterator by the current plugin
                for (MongoResultIterator it : ctx.getIterators()) {
                  if (it instanceof LazyMongoResultIterator laziIt && laziIt.isNotBound()) {
						// we cannot return lazy iterator that is not initialized as the initialization
						// should happen down this branch
						continue;
					}
					if (it.getQueryIdentifier() == object && (!it.isQuerySet() || it.isCloned()) && !it.isClosed()) {
						ctx.setPhase(ContextPhase.SEARCH_DEFINITION);
						return it;
					}
				}
			}

			Configuration config = resolveConfiguration(suffix);

			if (config == null)
				return StatementIterator.EMPTY;

			String connectString = config.getConnectionString();

			if (connectString == null || connectString.trim().length() == 0) {
				getLogger().error("Invalid connect parameters for MongoDB inst {}!", suffix);
				return StatementIterator.EMPTY;
			}
			// get
            ctx.setSearchBNode(entities.put(VF.createBNode(), Scope.REQUEST));
			ctx.setPhase(ContextPhase.SEARCH_DEFINITION);
			ctx.addContext(object);
			MongoResultIterator mainIterator = createMainIterator(config, ctx, object, 0);

			// check if we have lazy iterator that was just materialized by the newly created
			// main iterator. If so then we must return the lazy iterator here instead of the main one
			// this makes sure to set proper state to the proxy iterator instead only to the internal delegate
			// also the order in which the iterators are traversed also is important as this will bind
			// a lazy iterator to the newly created iterator
			// see: TestPluginMongoBasicQueries#shouldWorkTwoIndexes_doubleCustomInvertedGraphs()
            for (MongoResultIterator it : ctx.getIterators()) {
              if (it instanceof LazyMongoResultIterator laziIt && laziIt.getDelegate() == mainIterator)
					return it;
			}
			return mainIterator;
		}

		if (predicate == graphId) {

          if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			// we are not using the getIterator method as we want to do the selection by getGraphId only
			// and not by query identifier where if the graphId is not set the indexId will be used
			MongoResultIterator resultIterator = null;
			if (subject != 0) {
				// match by search subject, will enter on the second pass when the subject is bound
                for (MongoResultIterator it : ctx.getIterators()) {
					if (it.getSearchSubject() == subject && !it.isClosed()) {
						resultIterator = it;
						break;
					}
				}
			}
			if (resultIterator == null) {
				// not matched by subject, try finding an iterator that matches the same graphId, if already
				// defined by a model pattern defined before the query. In this case the iterator should be
				// lazy iterator that have the same graphId
                Iterator<MongoResultIterator> iter = ctx.getIterators().descendingIterator();
				while (iter.hasNext()) {
					MongoResultIterator curr = iter.next();
					if (curr.getGraphId() == object && !curr.isClosed()) {
						resultIterator = curr;
						break;
					}
				}
			}
			if (resultIterator == null) {
				// use the last created iterator as it probably is our best guess
                resultIterator = ctx.getIterators().getLast();
			}

			if (object != 0) {
				resultIterator.setGraphId(object);
				ctx.addContext(object);
			}

			return resultIterator.singletonIterator(queryId, object);
		}
		if (predicate == queryId) {
			String queryString = Utils.getString(entities, object);

            if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setQuery(queryString);
			return iter.singletonIterator(queryId, object);
		}
		if (predicate == projectionId) {
			String projectionString = Utils.getString(entities, object);

            if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}
			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setProjection(projectionString);
			return iter.singletonIterator(projectionId, object);
		}
		if (predicate == aggregationId) {
          if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			String aggregationString = Utils.getString(entities, object);

			if (aggregationString == null) {
				// make sure to mark the aggregate parameter as lazy set
				// this will ensure the main iterator would not be closed before setting this.
				MongoResultIterator iter = getIterator(subject, context, ctx);
				iter.setAggregation(null);
				return iter.singletonIterator(aggregationId, object);
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
		if (predicate == collationId){
          if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}
			String collationString = Utils.getString(entities,object);

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setCollation(collationString);
			return iter.singletonIterator(collationId, object);
		}
		if (predicate == batchSize) {
          if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}
			Integer batchSizeCfg = readBatchSize(object, entities);
			if (batchSizeCfg == null) {
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setDocumentsLimit(batchSizeCfg);
			return iter.singletonIterator(batchSize, object);
		}
		if (predicate == hintId) {
			String hintString = Utils.getString(entities, object);

			MongoResultIterator iter = getIterator(subject, context, ctx);
			iter.setHint(hintString);
			return iter.singletonIterator(hintId, object);
		}
		if (predicate == entityId) {
          if (ctx.getIterators() == null) {
				getLogger().error("iter not created yet");
				return StatementIterator.EMPTY;
			}

			MongoResultIterator iter = getIterator(subject, context, ctx);
			return iter.createEntityIter(entityId);
		}

		if (context != 0) {
			MongoResultIterator iterator;
			String suffix = Utils.matchPrefix(entities.get(context).stringValue(), NAMESPACE_INST);
			if (suffix != null && suffix.length() > 0) {
              if (ctx.getIterators() == null) {
					// no iterators up until this moment so we probably have model pattern before the actual query definition
					// try to create main iterator in advance or lazy iterator if custom graph is used
					iterator = createMainIterator(context, entities, ctx);
					if (iterator == null) {
						// invalid mongo configuration, already logged by the method above
						return StatementIterator.EMPTY;
					}
					return iterator.getModelIterator(subject, predicate, object);
				}

				// we have previous iterator, we must find suitable one from them
				// we can be here in the following cases:
				// - model pattern, before actual query
				// - model pattern, after actual query
				// - repeating model pattern after actual query
				// - repeating model pattern before actual query
				// the rules for picking existing vs new iterator are as follows:
				// - use existing iterator when:
				//	 - the iterator was created BEFORE the entering here (contextFirst == false)
				// - create new iterator when:
				//	 - there is NO iterator without model pattern or entity pattern
                boolean reuseIterators = subject != 0 || (ctx.getPhase() == ContextPhase.MODEL_ITERATION
                    && ctx.getPreviousPhase() != ContextPhase.MODEL_ITERATION);
				iterator = getIteratorOrNull(subject, context, ctx, reuseIterators);
				if (iterator == null) {
					iterator = createMainIterator(context, entities, ctx);
				}
				if (iterator == null) {
					// invalid mongo configuration, already logged by the method above
					return StatementIterator.EMPTY;
				}
				return iterator.getModelIterator(subject, predicate, object);
			}
		}
		return null;
	}

	private Integer readBatchSize(long object, Entities entities) {
		Integer batchSizeCfg = Utils.getInteger(entities, object);
		if (batchSizeCfg == null || batchSizeCfg < 0) {
			getLogger().error("Invalid batch size configuration: {}",
							Utils.getString(entities, object));
				return null;
		}
		if (batchSizeCfg >= MAX_BATCH_SIZE) {
			if (MAX_BATCH_SIZE == 0) {
				getLogger().warn("Batch document functionality is disabled. Ignoring {} configuration.",
								BATCH);
			} else {
				getLogger().warn("Batch size {} exceeds maximum {}. Using default size.",
								Utils.getString(entities, object), MAX_BATCH_SIZE);
			}
			batchSizeCfg = MAX_BATCH_SIZE;
		}
		return batchSizeCfg;
	}

	private Configuration resolveConfiguration(String suffix) {
		return configMap.computeIfAbsent(suffix, indexName -> {
					File indexInst = new File(getDataDir(), indexName);
					if (!indexInst.exists()) {
						getLogger().error("MongoDB service {} not connected!", indexName);
						return null;
					}
					File propertiesFile = new File(indexInst, MONGODB_PROPERTIES);
					try {
						return Configuration.fromFile(propertiesFile);
					} catch (IOException e) {
						getLogger().error("Cannot get MongoDB connect parameters {}. Exception {}!", indexName, e);
						return null;
					}
				});
	}

	@Override
	public RequestContext preprocess(Request request) {
      PluginRequestContext ctx = new PluginRequestContext();
      ctx.setRequest(request);
      return ctx;
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

			Configuration config = configMap.computeIfAbsent(suffix, indexName -> {
				File indexFolder = new File(getDataDir(), indexName);
				if (!indexFolder.exists()) {
					getLogger().info("Creating a new service in MongoDB: {}", indexName);
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

    protected MongoResultIterator getIterator(long subject, long context, PluginRequestContext ctx) {
		MongoResultIterator iterator = getIteratorOrNull(subject, context, ctx, true);
		if (iterator == null) {
			// when no matching iterator is found we can return the last created, not closed, iterator
			// as this is our best bet to what we can use
            Iterator<MongoResultIterator> iter = ctx.getIterators().descendingIterator();
			while (iter.hasNext()) {
				MongoResultIterator next = iter.next();
				if (!next.isClosed()) {
					return next;
				}
			}
            return ctx.getIterators().getLast();
		}
		return iterator;
	}

    protected MongoResultIterator getIteratorOrNull(long subject, long context, PluginRequestContext ctx,
        boolean canReuseIterators) {
		if (subject != 0) {
        for (MongoResultIterator it : ctx.getIterators()) {
				if (it.getSearchSubject() == subject && !it.isClosed()) {
					return it;
				}
			}
		}
		if (context != 0) {
          Iterator<MongoResultIterator> iter = ctx.getIterators().descendingIterator();
			while (iter.hasNext()) {
				MongoResultIterator curr = iter.next();
				if (curr.getQueryIdentifier() == context && !curr.isClosed()) {
					if (!canReuseIterators
									&& curr.isContextFirst()
									&& curr.isQuerySet()
									&& curr.isModelIteratorCreated()
									&& curr.isEntityIteratorCreated()
					) {
						// we do not want to reuse already fully produced/used iterator, so skip it
						continue;
					}
					return curr;
				}
			}
		}
		return null;
	}

    protected MongoResultIterator createMainIterator(long graphId, Entities entities, PluginRequestContext ctx) {
		String suffix = Utils.matchPrefix(entities.get(graphId).stringValue(), NAMESPACE_INST);
		if (StringUtils.isBlank(suffix)) {
			getLogger().error("Invalid MongoDB inst {}!", suffix);
			return null;
		}
		Configuration config = resolveConfiguration(suffix);
		if (config == null) {
			// custom context placed before the query definition itself: '?search a index:spb100 ; :graph index:spb500 ;'
			// so we create lazy resolvable iterator that will map to actual iterator when
			// the query part is reached. until then we will use this one as replacement
			MongoResultIterator resultIterator = new LazyMongoResultIterator(graphId, ctx);
			// register the iterator as any main iterator so it can be accessed by other methods
			ctx.addIterator(resultIterator);
			ctx.setPhase(ContextPhase.MODEL_ITERATION);
			return resultIterator;
		}
		if (StringUtils.isBlank(config.getConnectionString())) {
			getLogger().error("Invalid connect parameters for MongoDB inst {}!", suffix);
			return null;
		}
		// get
        ctx.setSearchBNode(entities.put(VF.createBNode(), Scope.REQUEST));
		ctx.addContext(graphId);

		MongoResultIterator previousIterator = null;
        if (ctx.getIterators() != null && graphId != 0) {
          Iterator<MongoResultIterator> iter = ctx.getIterators().descendingIterator();
			while (iter.hasNext()) {
				MongoResultIterator curr = iter.next();
				if (curr.getQueryIdentifier() == graphId && curr.isClosed()) {
					previousIterator = curr;
					break;
				}
			}
		}

		// we are here because the there is graph pattern before the actual query definition
		// the context is known and it matches the query type '?search a index:spb100 ;'
		// the created iterator will be added to the context in the method that creates it
		MongoResultIterator mainIterator = createMainIterator(config, ctx, 0, graphId);
		mainIterator.setContextFirst(true);
		ctx.setPhase(ContextPhase.MODEL_ITERATION);
		if (previousIterator != null) {
			mainIterator.setCloned(true);
			mainIterator.setQuery(previousIterator.getQuery());
			mainIterator.setAggregation(previousIterator.getAggregation());
			mainIterator.setCollation(previousIterator.getCollation());
			mainIterator.setHint(previousIterator.getHint());
			mainIterator.setProjection(previousIterator.getProjection());
		}
		return mainIterator;
	}

    protected MongoResultIterator createMainIterator(Configuration configuration, PluginRequestContext ctx,
        long indexId, long graphId) {
		String connect = configuration.getConnectionString();
		String database = configuration.getDatabase();
		String collection = configuration.getCollection();
		Optional<MongoCredential> mongoCredential = configuration.getMongoCredential();

		MongoClient client = mongoClients.computeIfAbsent(
				connect + mongoCredential.map(credential -> credential.getUserName() + "_" + credential.getSource()).orElse(""), s -> {
			MongoClientSettings.Builder builder = MongoClientSettings.builder();

			builder.applyConnectionString(new ConnectionString(connect));
			mongoCredential.ifPresent(builder::credential);

			return MongoClients.create(builder.build());
		});


        MongoResultIterator ret =
            new MongoResultIterator(this, client, database, collection, ctx.getCache(), ctx.getSearchBNode());
        ret.setEntities(ctx.getEntities());
		ret.setIndexId(indexId);
		// by default set the graphId to point to the search name
		ret.setGraphId(graphId);
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
