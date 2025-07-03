package com.ontotext.trree.plugin.mongodb.request;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.bson.Document;

/**
 * Caches the aggregate and find calls to mongo in each request.
 */
public class RequestCache {

  private static final int MAX_ITEMS = 100;
  private static final float FACTOR = 0.75f;

  private Map<Integer, AggregateIterable<Document>> aggregateCache = boundCache(MAX_ITEMS);
  private Map<Integer, FindIterable<Document>> findCache = boundCache(MAX_ITEMS);

  public static <K, V> Map<K, V> boundCache(final int maxSize) {
    return new LinkedHashMap<>((int) (maxSize / FACTOR), FACTOR, true) {

      private static final long serialVersionUID = 1L;

      @Override
      protected boolean removeEldestEntry(Entry<K, V> eldest) {
        return size() > maxSize;
      }
    };
  }

  public AggregateIterable<Document> getAggregation(
      MongoCollection<Document> coll, String database, String collection, List<Document> aggregation) {
    return aggregateCache.computeIfAbsent(
        database.hashCode() ^ collection.hashCode() ^ aggregation.hashCode(),
        k -> coll.aggregate(aggregation));
  }

  public FindIterable<Document> getFind(
      MongoCollection<Document> coll, String database, String collection, Document document) {
    return findCache.computeIfAbsent(
        database.hashCode() ^ collection.hashCode() ^ document.hashCode(),
        k -> coll.find(document));
  }
}
