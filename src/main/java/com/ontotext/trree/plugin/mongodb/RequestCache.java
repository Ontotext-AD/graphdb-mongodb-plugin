package com.ontotext.trree.plugin.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;

/**
 * Caches the aggregate and find calls to mongo in each request.
 */
public class RequestCache {

	protected HashMap<Integer, AggregateIterable<Document>> aggregateCache = new HashMap<>();
	protected HashMap<Integer, FindIterable<Document>> findCache = new HashMap<>();

	AggregateIterable<Document> getAggregation(MongoCollection coll, String database, String collection, List<Document> aggregation) {
		return aggregateCache
				.computeIfAbsent(database.hashCode() ^ collection.hashCode() ^ aggregation.hashCode()
						, s -> {
							return coll.aggregate(aggregation);
						});
	}

	FindIterable<Document> getFind(MongoCollection coll, String database, String collection, Document find) {
		return findCache
				.computeIfAbsent(database.hashCode() ^ collection.hashCode() ^ find.hashCode()
						, s -> {
							return coll.find(find);
						});
	}
}
