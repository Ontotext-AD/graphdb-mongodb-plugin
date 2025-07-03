package com.ontotext.trree.plugin.mongodb.request;

import com.ontotext.trree.plugin.mongodb.iterator.MongoResultIterator;
import com.ontotext.trree.sdk.Entities;
import com.ontotext.trree.sdk.Request;
import com.ontotext.trree.sdk.RequestContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * This is the context implementation where the plugin stores currently running patterns it just keeps some values using
 * sting keys for further access.
 */
public class PluginRequestContext implements RequestContext {

  private Request request;
  private RequestCache cache = new RequestCache();
  private Map<String, Object> map = new HashMap<>();
  private LinkedList<MongoResultIterator> iterators;
  private Set<Long> contexts = new HashSet<>();
  private Entities entities;
  private long searchBNode;
  private ContextPhase previousPhase = ContextPhase.INITIAL;
  private ContextPhase phase = ContextPhase.INITIAL;

  @Override
  public Request getRequest() {
    return request;
  }

  @Override
  public void setRequest(Request request) {
    this.request = request;
  }

  public RequestCache getCache() {
    return cache;
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

  public LinkedList<MongoResultIterator> getIterators() {
    return iterators;
  }

  public void addIterator(MongoResultIterator iter) {
    if (this.iterators == null) {
      this.iterators = new LinkedList<>();
    }
    this.iterators.add(iter);
  }

  public void addContext(long ctx) {
    contexts.add(ctx);
  }

  public Set<Long> getContexts() {
    return contexts;
  }

  public Entities getEntities() {
    return entities;
  }

  public void setEntities(Entities entities) {
    this.entities = entities;
  }

  public long getSearchBNode() {
    return searchBNode;
  }

  public void setSearchBNode(long searchBNode) {
    this.searchBNode = searchBNode;
  }

  public ContextPhase getPreviousPhase() {
    return previousPhase;
  }

  public ContextPhase getPhase() {
    return phase;
  }

  public void setPhase(ContextPhase phase) {
    previousPhase = this.phase;
    this.phase = phase;
  }
}
