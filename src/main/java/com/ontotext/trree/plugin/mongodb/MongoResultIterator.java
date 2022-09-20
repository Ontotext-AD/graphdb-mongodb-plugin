package com.ontotext.trree.plugin.mongodb;

import com.mongodb.MongoSecurityException;
import com.mongodb.client.*;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.Entities.Scope;
import com.ontotext.trree.sdk.PluginException;
import com.ontotext.trree.sdk.StatementIterator;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoResultIterator extends StatementIterator {

	private static final String CUSTOM_NODE = "custom";

	private String query, projection, hint, database, collection;
	private Collation collation;
	private List<Document> aggregation = null;
	private long searchSubject;
	// custom graph id, if not present should equal to indexId
	private long graphId;
	// the id of the index, could be shared among multiple iterators
	private long indexId;
	private boolean initialized = false;
	private boolean initializedByEntityIterator = false;
	private boolean searchDone = false;
	private MongoClient client;
	private MongoDatabase db;
	private MongoCollection<Document> coll;
	private MongoCursor<Document> iter;
	private Model currentRDF;
	private Entities entities;
	private MongoDBPlugin plugin;
	private RequestCache cache;

	private boolean contextFirst = false;
  private boolean cloned = false;
	private boolean entityIteratorCreated = false;
	private boolean modelIteratorCreated = false;
	private boolean interrupted = false;
	private boolean closed = false;

	public MongoResultIterator(MongoDBPlugin plugin, MongoClient client, String database, String collection, RequestCache cache, long searchsubject) {
		this.cache = cache;
		this.plugin = plugin;
		this.client = client;
		this.database = database;
		this.collection = collection;
		this.searchSubject = searchsubject;
	}

	@Override
	public boolean next() {
		boolean ret = false;
		if (!initialized) {
			subject = searchSubject;
			// we cannot call initialize here as this method is called before actual query parameters are set
			// in order to populate the binding values in the queries
			ret = true;
			initialized = true;
		} else if (initializedByEntityIterator && !searchDone) {
			// the the graph pattern is located before the search query
			// the entity iterator will perform the search and return the results, but this iterator
			// must also return true at least once to tell the engine to read the query variables itself
			// otherwise they will be left unbound and nothing will be returned as result
			// so with this here we return true once when the connector is initialized via the entity iterator
			subject = searchSubject;
			searchDone = true;
			ret = true;
		}

		return ret;
	}

	protected boolean initialize() {
		if (query == null && aggregation == null)
			throw new PluginException("There is no search query for Mongo. Please use either of the predicates: " +
					Arrays.asList(MongoDBPlugin.QUERY.toString(), MongoDBPlugin.AGGREGATION.toString()));

		try {
			if (client != null) {
				db = client.getDatabase(database);
				coll = db.getCollection(collection);

				// If aggregation is used it will take precedence over query + projection
				if (aggregation != null) {
					AggregateIterable<Document> res = cache.getAggregation(coll, database, collection, aggregation);
					if (hint != null) {
						res.hint(Document.parse(hint));
					}
					if (collation != null) {
						res.collation(collation);
					}
					iter = res.iterator();
				} else {
					FindIterable<Document> res = cache.getFind(coll, database, collection, Document.parse(query));
					if (projection != null) {
						res = res.projection(Document.parse(projection));
					}
					if (hint != null) {
						res.hint(Document.parse(hint));
					}
					if (collation != null) {
						res.collation(collation);
					}

					iter = res.iterator();
				}
				initialized = true;
			}
		} catch (MongoSecurityException ex) {
			plugin.getLogger().error("Could not connect to mongo", ex);
			throw new PluginException("Could not connect to MongoDB. Please make sure you are using correct authentication. " + ex.getMessage());
		}
		return iter != null && iter.hasNext();
	}

	@Override
	public void close() {
    closed = true;
		if (iter != null)
			iter.close();
		iter = null;
		coll = null;
		db = null;
		client = null;

		interrupted = true;
		if (currentRDF != null) {
			currentRDF.clear();
			currentRDF = null;
		}
		initialized = false;
		initializedByEntityIterator = false;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public StatementIterator singletonIterator(long predicate, long object) {
		return StatementIterator.create(getSearchSubject(), predicate, object, 0);
	}

	public void setProjection(String projectionString) {
		this.projection = projectionString;
	}

	public StatementIterator createEntityIter(long pred) {
		setEntityIteratorCreated(true);
		return new StatementIterator() {
			boolean initializedE = false;

			@Override
			public boolean next() {
				if (!initializedE) {
					initialize();
					initializedE = true;
				}
				if (hasSolution()) {
					this.subject = searchSubject;
					this.predicate = pred;
					advance();
					this.object = MongoResultIterator.this.object;
					return true;
				}
				return false;
			}

			@Override
			public void close() {
			}

		};
	}

	private void advance() {
		Document doc = iter.next();

		if (interrupted)
			return;
		
		String entity = null;
		if (doc.containsKey("@graph")) {
			Object item = doc.get("@graph");
			Document graphDoc;
			if (item != null) {
				if (item instanceof List) {
					List<?> listItem = (List<?>)item;
					if (!listItem.isEmpty() && listItem.get(0) instanceof Document) {
						graphDoc = (Document)listItem.get(0);
						entity = graphDoc.getString("@id");
						if (listItem.size() > 1) {
							plugin.getLogger().warn("Multiple graphs in mongo document. Selecting the first one for entity:	" + entity);
						}
					} else {
						plugin.getLogger().warn("Value of @graph must be a valid document in mongo document.");
					}
				} else if (item instanceof Document) {
					graphDoc = (Document)item;
					entity = graphDoc.getString("@id");
				} else {
					plugin.getLogger().warn("@graph must be a document or list of documents in mongo document.");
				}
			}
		}
		if (entity == null){
			entity = doc.getString("@id");
		}
		Resource v = null;
		if (entity != null) {
			try {
				v = plugin.vf.createIRI(entity);
			} catch (IllegalArgumentException e) {
				Document contexts = (Document)doc.get("@context");
				if (contexts != null) {
					String base = contexts.getString("@base");
					if (base != null) {
						try {
							v = plugin.vf.createIRI(base, entity);
						} catch (IllegalArgumentException e2) {
							// ignore the exception
						}
					}
				}
				if (v == null)
					throw e;
			}
		} else {
			v = plugin.vf.createBNode();
		}
		long id = entities.resolve(v);
		if (id == 0) {
			id = entities.put(v, Scope.REQUEST);
		}
		this.object = id;
		try {
			//Relaxed mode Json conversion is needed for canonical MongoDB v2 Json document values
			currentRDF = Rio.parse(new StringReader(
					doc.toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build(),
							new EncoderWrapper(new DocumentCodec()))), "http://base.org", RDFFormat.JSONLD);

			Object customNode = doc.get(CUSTOM_NODE);
			if (customNode != null && customNode instanceof Document) {
				for (Map.Entry<String, Object> val : ((Document) customNode).entrySet()) {
					currentRDF.add(v, plugin.vf.createIRI(MongoDBPlugin.NAMESPACE_INST, val.getKey()), plugin.vf.createLiteral(val.getValue().toString()));
				}
			}
		} catch (RDFParseException | UnsupportedRDFormatException | IOException e) {
			iter.close();
			plugin.getLogger().error("Could not parse mongo document", e);
		}
	}

	protected boolean hasSolution() {
		return !interrupted && iter != null && iter.hasNext();
	}

	public StatementIterator getModelIterator(final long subject, final long predicate, final long object) {
		setModelIteratorCreated(true);
		Resource s = subject == 0 ? null : (Resource) entities.get(subject);
		IRI p = predicate == 0 ? null : (IRI) entities.get(predicate);
		Value o = object == 0 ? null : entities.get(object);
		return new StatementIterator() {
			Iterator<Statement> local = null;

			@Override
			public boolean next() {
				if (currentRDF == null) {
          return false;
				}
				if (local == null)
					local = currentRDF.filter(s, p, o).iterator();
				boolean has = local.hasNext();
				if (has) {
					Statement st = local.next();
					this.subject = entities.resolve(st.getSubject());
					if (this.subject == 0) {
						this.subject = entities.put(st.getSubject(), Scope.REQUEST);
					}
					this.predicate = entities.resolve(st.getPredicate());
					if (this.predicate == 0) {
						this.predicate = entities.put(st.getPredicate(), Scope.REQUEST);
					}
					this.object = entities.resolve(st.getObject());
					if (this.object == 0) {
						this.object = entities.put(st.getObject(), Scope.REQUEST);
					}
				}
				return has;
			}

			@Override
			public void close() {
			}
		};
	}

	public void setAggregation(List<Document> aggregation) {
		this.aggregation = aggregation;
	}

	public void setGraphId(long graphId) {
		this.graphId = graphId;
	}

	public long getQueryIdentifier() {
		long gid = getGraphId();
		return gid != 0 ? gid : getIndexId();
	}

	public long getGraphId() {
		return graphId;
	}

	public long getIndexId() {
		return indexId;
	}

	public void setIndexId(long indexId) {
		this.indexId = indexId;
	}

	public long getSearchSubject() {
		return searchSubject;
	}

	public void setEntities(Entities entities) {
		this.entities = entities;
	}

	public void setHint(String hintString) {
		this.hint = hintString;
	}

	public void setCollation(String collationString){
		setCollation(createCollation(collationString));
	}

	public Collation getCollation() {
		return collation;
	}

	private Collation createCollation(String collationString) {
		Document doc = Document.parse(collationString);
		Collation.Builder builder = Collation.builder();
		builder.locale(doc.getString("locale"));

		if (doc.containsKey("caseLevel")){
			builder.caseLevel(doc.getBoolean("caseLevel"));
		}

		if (doc.containsKey("caseFirst")){
			builder.collationCaseFirst(CollationCaseFirst.fromString(doc.getString("caseFirst")));
		}

		if (doc.containsKey("strength")){
			builder.collationStrength(CollationStrength.fromInt(doc.getInteger("strength")));
		}

		if (doc.containsKey("numericOrdering")) {
			builder.numericOrdering(doc.getBoolean("numericOrdering"));
		}

		if (doc.containsKey("alternate")) {
			builder.collationAlternate(
					CollationAlternate.fromString(doc.getString("alternate")));
		}

		if (doc.containsKey("maxVariable")) {
			builder.collationMaxVariable(
					CollationMaxVariable.fromString(doc.getString("maxVariable")));
		}

		if (doc.containsKey("normalization")) {
			builder.normalization(doc.getBoolean("normalization"));
		}

		if (doc.containsKey("backwards")) {
			builder.backwards(doc.getBoolean("backwards"));
		}

		return builder.build();
	}

	public boolean isQuerySet() {
		return query != null || aggregation != null;
	}

	public boolean isContextFirst() {
		return contextFirst;
	}

	public void setContextFirst(boolean contextFirst) {
		this.contextFirst = contextFirst;
	}

	public void setModelIteratorCreated(boolean modelIteratorCreated) {
		this.modelIteratorCreated = modelIteratorCreated;
	}

	public void setEntityIteratorCreated(boolean entityIteratorCreated) {
		this.entityIteratorCreated = entityIteratorCreated;
	}

	public boolean isEntityIteratorCreated() {
		return entityIteratorCreated;
	}

	public boolean isModelIteratorCreated() {
		return modelIteratorCreated;
	}

	public String getQuery() {
		return query;
	}

	public String getProjection() {
		return projection;
	}

	public String getHint() {
		return hint;
	}

	public void setCollation(Collation collation) {
		this.collation = collation;
	}

	public List<Document> getAggregation() {
		return aggregation;
	}

	public boolean isClosed() {
	  return closed;
  }

	protected void reset() {
		query = null;
		aggregation = null;
		projection = null;
		hint = null;
		modelIteratorCreated = false;
		entityIteratorCreated = false;
	}

  public boolean isCloned() {
    return cloned;
  }

  public void setCloned(boolean cloned) {
    this.cloned = cloned;
  }
}
