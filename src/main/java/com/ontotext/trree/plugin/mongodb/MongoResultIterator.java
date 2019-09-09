package com.ontotext.trree.plugin.mongodb;

import com.mongodb.MongoSecurityException;
import com.mongodb.client.*;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.Entities.Scope;
import com.ontotext.trree.sdk.PluginException;
import com.ontotext.trree.sdk.StatementIterator;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
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

	String query, projection, hint, database, collection;
	List<Document> aggregation = null;
	long searchSubject;
	long graphId;
	boolean initialized = false;
	MongoClient client;
	MongoDatabase db;
	MongoCollection<Document> coll;
	MongoCursor<Document> iter;
	private Model currentRDF;
	Entities entities;
	MongoDBPlugin plugin;
	RequestCache cache;

	boolean interrupted = false;

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
			ret = true;
			initialized = true;
		}

		return ret;
	}

	protected boolean initialize() {
		if (query == null && aggregation == null)
			throw new PluginException("There is no search query for Mongo. Please use either of the predicates: " +
					Arrays.asList(new String[] {MongoDBPlugin.QUERY.toString(), MongoDBPlugin.AGGREGATION.toString()}));

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
					iter = res.iterator();
				} else {
					FindIterable<Document> res = cache.getFind(coll, database, collection, Document.parse(query));
					if (projection != null) {
						res = res.projection(Document.parse(projection));
					}
					if (hint != null) {
						res.hint(Document.parse(hint));
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
		if (iter != null)
			iter.close();
		iter = null;
		coll = null;
		db = null;
		client = null;

		interrupted = true;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public StatementIterator singletonIterator(long predicate, long object) {
		return StatementIterator.create(searchSubject, predicate, object, 0);
	}

	public void setProjection(String projectionString) {
		this.projection = projectionString;
	}

	public StatementIterator createEntityIter(long pred) {
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

		String entity;
		List graph = doc.get("@graph", List.class);
		if (graph != null && graph.size() > 0) {
			entity = ((Document) graph.get(0)).getString("@id");
			if (graph.size() > 1) {
				plugin.getLogger().warn("Multiple graphs in mongo document. Selecting the first one for entity:  " + entity);
			}
		} else {
			entity = doc.getString("@id");
		}
		Resource v;
		if (entity != null) {
			v = plugin.vf.createIRI(entity);
		} else {
			v = plugin.vf.createBNode();
		}
		long id = entities.resolve(v);
		if (id == 0) {
			id = entities.put(v, Scope.REQUEST);
		}
		this.object = id;
		try {
			currentRDF = Rio.parse(new StringReader(doc.toJson(new EncoderWrapper(new DocumentCodec()))), "http://base.org", RDFFormat.JSONLD);

			Object customNode = doc.get(CUSTOM_NODE);
			if (customNode != null && customNode instanceof Document) {
				for (Map.Entry<String, Object> val : ((Document) customNode).entrySet()) {
					currentRDF.add(v, plugin.vf.createIRI(plugin.NAMESPACE_INST, val.getKey()), plugin.vf.createLiteral(val.getValue().toString()));
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
		Resource s = subject == 0 ? null : (Resource) entities.get(subject);
		IRI p = predicate == 0 ? null : (IRI) entities.get(predicate);
		Value o = object == 0 ? null : entities.get(object);
		return new StatementIterator() {
			Iterator<Statement> local = null;

			@Override
			public boolean next() {
				if (currentRDF == null)
					return false;
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

	public void setHint(String hintString) {
		this.hint = hintString;
	}
}
