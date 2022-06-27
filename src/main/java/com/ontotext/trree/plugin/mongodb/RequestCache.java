package com.ontotext.trree.plugin.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Caches the aggregate and find calls to mongo in each request.
 */
public class RequestCache {

	final int MAX_ITEMS = 100;
	final static float FACTOR = 0.75f;
	protected Map<Integer, AggregateIterable<Document>> aggregateCache = boundCache(MAX_ITEMS);
	protected Map<Integer, FindIterable<Document>> findCache = boundCache(MAX_ITEMS);
	
	public static <K,V> Map<K,V> boundCache(final int maxSize) {
        return new LinkedHashMap<K,V>((int)(maxSize/FACTOR), FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
            	return size() > maxSize;
            }
        };
    }
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
