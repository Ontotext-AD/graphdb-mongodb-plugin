package com.ontotext.trree.plugin.mongodb.iterator;

import com.mongodb.client.model.Collation;
import com.ontotext.trree.plugin.mongodb.request.PluginRequestContext;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.StatementIterator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.bson.Document;

/**
 * Lazy initialized iterator used to represent model pattern selections, defined before the actual query definition.
 * Until the query is defined and added to the given context the iterator will try its best to fake the absence of the
 * real iterator. The only known element of the iterator is the context id that can be used to resolve the actual
 * matching query.
 *
 * @author BBonev
 */
public class LazyMongoResultIterator extends MongoResultIterator {

  private MongoResultIterator delegate;

  private final PluginRequestContext ctx;

  public LazyMongoResultIterator(long context, PluginRequestContext ctx) {
    super(null, null, null, null, null, 0);
    this.ctx = ctx;
    super.setGraphId(context);
  }

  public boolean isNotBound() {
    return getDelegate() == null;
  }

  @Override
  public boolean next() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return false;
    }

    boolean hasNext = delegate.next();
    this.subject = delegate.subject;
    this.predicate = delegate.predicate;
    this.object = delegate.object;
    this.context = delegate.context;
    return hasNext;
  }

  @Override
  public StatementIterator getModelIterator(long subjectM, long predicateM, long objectM) {
    setModelIteratorCreated(true);
    return new StatementIterator() {
      private StatementIterator modelIterator;

      @Override
      public boolean next() {
        if (modelIterator == null) {
          MongoResultIterator delegate = getDelegate();
          if (delegate == null) {
            return false;
          }
          modelIterator = delegate.getModelIterator(subjectM, predicateM, objectM);
        }

        boolean hasNext = modelIterator.next();
        this.subject = modelIterator.subject;
        this.predicate = modelIterator.predicate;
        this.object = modelIterator.object;
        this.context = modelIterator.context;
        return hasNext;
      }

      @Override
      public void close() {
        if (modelIterator != null) {
          modelIterator.close();
        }
      }
    };
  }

  public MongoResultIterator getDelegate() {
    if (delegate == null) {
      long graphId = super.getGraphId();
      long searchSubject = super.getSearchSubject();
      delegate = getIterator(searchSubject, graphId, ctx);
    }
    return delegate;
  }

  private MongoResultIterator getIterator(long subject, long context, PluginRequestContext ctx) {
    if (subject != 0) {
      // search by subject for possible delegate match
      for (MongoResultIterator it : ctx.getIterators()) {
        if (!(it instanceof LazyMongoResultIterator) && it.getSearchSubject() == subject) {
          if (isAlreadyDelegateToSomeoneElse(ctx, it)) {
            // the current iterator is already proxied by other iterator, they cannot be shared between proxies
            continue;
          }
          return it;
        }
      }
    }

    if (context != 0) {
      // check for the latest non delegate iterators without assigned delegate that share the same context
      Iterator<MongoResultIterator> iter = ctx.getIterators().descendingIterator();
      while (iter.hasNext()) {
        MongoResultIterator curr = iter.next();
        if (!(curr instanceof LazyMongoResultIterator) && curr.getGraphId() == context) {
          if (isAlreadyDelegateToSomeoneElse(ctx, curr)) {
            // the current iterator is already proxied by other iterator, they cannot be shared between proxies
            continue;
          }
          return curr;
        }
      }
    }

    // when the query defines custom graph pattern the above methods will fail to match until the
    // graph predicate is visited
    Iterator<MongoResultIterator> iter = ctx.getIterators().descendingIterator();
    while (iter.hasNext()) {
      MongoResultIterator curr = iter.next();
      if (!(curr instanceof LazyMongoResultIterator)) {
        if (isAlreadyDelegateToSomeoneElse(ctx, curr)) {
          // the current iterator is already proxied by other iterator, they cannot be shared between proxies
          continue;
        }
        return curr;
      }
    }

    return null;
  }

  private boolean isAlreadyDelegateToSomeoneElse(PluginRequestContext ctx, MongoResultIterator currentPick) {
    return ctx.getIterators().stream()
        .anyMatch(it -> it instanceof LazyMongoResultIterator lazyIt && lazyIt.delegate == currentPick);
  }

  @Override
  public void close() {
    super.close();
    if (delegate != null) {
      delegate.close();
    }
  }

  @Override
  public StatementIterator createEntityIter(long pred) {
    // we should have actual iterator instance when entity iterator is requested
    return getDelegate().createEntityIter(pred);
  }

  @Override
  public void setQuery(String query) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setQuery(query);
    }
    super.setQuery(query);
  }

  @Override
  public void setProjection(String projectionString) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setProjection(projectionString);
    }
    super.setProjection(projectionString);
  }

  @Override
  public void setAggregation(List<Document> aggregation) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setAggregation(aggregation);
    }
    super.setAggregation(aggregation);
  }

  @Override
  public void setGraphId(long graphId) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setGraphId(graphId);
    }
    super.setGraphId(graphId);
  }

  @Override
  public long getGraphId() {
    long graphId = super.getGraphId();
    if (graphId != 0) {
      return graphId;
    }
    return resolveValue(MongoResultIterator::getGraphId);
  }

  @Override
  public void setIndexId(long indexId) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setIndexId(indexId);
    }
    super.setIndexId(indexId);
  }

  @Override
  public void setDocumentsLimit(int documentsLimit) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setDocumentsLimit(documentsLimit);
    }
    super.setDocumentsLimit(documentsLimit);
  }

  @Override
  public int getDocumentsLimit() {
    int limit = super.getDocumentsLimit();
    if (limit != 0) {
      return limit;
    }
    return resolveValue(MongoResultIterator::getDocumentsLimit);
  }

  @Override
  public long getIndexId() {
    long indexId = super.getIndexId();
    if (indexId != 0) {
      return indexId;
    }
    return resolveValue(MongoResultIterator::getIndexId);
  }

  @Override
  public long getSearchSubject() {
    long searchSubject = super.getSearchSubject();
    if (searchSubject != 0) {
      return searchSubject;
    }
    return resolveValue(MongoResultIterator::getSearchSubject);
  }

  @Override
  public void setEntities(Entities entities) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      delegate.setEntities(entities);
    }
    super.setEntities(entities);
  }

  @Override
  public void setHint(String hintString) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setHint(hintString);
    }
    super.setHint(hintString);
  }

  @Override
  public void setCollation(String collationString) {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      super.setCollation(collationString);
    } else {
      getDelegate().setCollation(collationString);
    }
  }

  @Override
  public Collation getCollation() {
    Collation collation = super.getCollation();
    if (collation != null) {
      return collation;
    }
    return resolveValue(MongoResultIterator::getCollation);
  }

  @Override
  public boolean isQuerySet() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return super.isQuerySet();
    }
    return delegate.isQuerySet();
  }

  @Override
  public boolean isContextFirst() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return super.isContextFirst();
    }
    return delegate.isContextFirst();
  }

  @Override
  public void setContextFirst(boolean contextFirst) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setContextFirst(contextFirst);
    }
    super.setContextFirst(contextFirst);
  }

  @Override
  public void setModelIteratorCreated(boolean modelIteratorCreated) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setModelIteratorCreated(modelIteratorCreated);
    }
    super.setModelIteratorCreated(modelIteratorCreated);
  }

  @Override
  public void setEntityIteratorCreated(boolean entityIteratorCreated) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      getDelegate().setEntityIteratorCreated(entityIteratorCreated);
    }
    super.setEntityIteratorCreated(entityIteratorCreated);
  }

  @Override
  public boolean isEntityIteratorCreated() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return super.isEntityIteratorCreated();
    }
    return delegate.isEntityIteratorCreated();
  }

  @Override
  public boolean isModelIteratorCreated() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return super.isModelIteratorCreated();
    }
    return delegate.isModelIteratorCreated();
  }

  @Override
  public String getQuery() {
    String query = super.getQuery();
    if (query != null) {
      return query;
    }
    return resolveValue(MongoResultIterator::getQuery);
  }

  @Override
  public String getProjection() {
    String projection = super.getProjection();
    if (projection != null) {
      return projection;
    }
    return resolveValue(MongoResultIterator::getProjection);
  }

  @Override
  public String getHint() {
    String hint = super.getHint();
    if (hint != null) {
      return hint;
    }
    return resolveValue(MongoResultIterator::getHint);
  }

  @Override
  public void setCollation(Collation collation) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      delegate.setCollation(collation);
    }
    super.setCollation(collation);
  }

  @Override
  public List<Document> getAggregation() {
    List<Document> aggregation = super.getAggregation();
    if (aggregation != null) {
      return aggregation;
    }
    return resolveValue(MongoResultIterator::getAggregation);
  }

  @Override
  public boolean isClosed() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return super.isClosed();
    }
    return delegate.isClosed();
  }

  @Override
  public boolean isCloned() {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return super.isCloned();
    }
    return delegate.isCloned();
  }

  @Override
  public void setCloned(boolean cloned) {
    MongoResultIterator delegate = getDelegate();
    if (delegate != null) {
      delegate.setCloned(cloned);
    }
    super.setCloned(cloned);
  }

  private <T> T resolveValue(Function<MongoResultIterator, T> supplier) {
    MongoResultIterator delegate = getDelegate();
    if (delegate == null) {
      return null;
    }
    return supplier.apply(delegate);
  }
}
