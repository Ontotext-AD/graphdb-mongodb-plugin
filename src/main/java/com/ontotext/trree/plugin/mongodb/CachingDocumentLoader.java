package com.ontotext.trree.plugin.mongodb;

import com.github.jsonldjava.core.DocumentLoader;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.RemoteDocument;
import com.github.jsonldjava.utils.JsonUtils;
import java.io.Closeable;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Custom document loader that will cache the remote documents. This will not cache regular JSON-LD
 * documents but rather only remote {@code "@contexts": "http://remote.context.file"}
 *
 * @author <a href="mailto:borislav.bonev@ontotext.com">Borislav Bonev</a>
 * @since 19/10/2022
 */
public class CachingDocumentLoader extends DocumentLoader implements Closeable {

  // as the document instances are immutable we can cache them whole
  private final Map<String, RemoteDocument> documentCache = new LinkedHashMap<String, RemoteDocument>() {
    @Override protected boolean removeEldestEntry(Map.Entry eldest) {
      // if the cache reaches 1000 elements then the first (oldest) entity will be dropped from the cache
      // this is mainly a failsafe in case of someone tries to fill up the memory with random contexts
      return documentCache.size() > 1000;
    }
  };

  @Override
  public RemoteDocument loadDocument(String url) throws JsonLdError {
    if (documentCache.containsKey(url)) {
      try {
        return documentCache.get(url);
      } catch (final Exception e) {
        throw new JsonLdError(JsonLdError.Error.LOADING_INJECTED_CONTEXT_FAILED, url, e);
      }
    } else {
      final String disallowRemote = System
              .getProperty(DocumentLoader.DISALLOW_REMOTE_CONTEXT_LOADING);
      if ("true".equalsIgnoreCase(disallowRemote)) {
        throw new JsonLdError(JsonLdError.Error.LOADING_REMOTE_CONTEXT_FAILED,
                "Remote context loading has been disallowed (url was " + url + ")");
      }

      try {
        RemoteDocument remoteDocument = new RemoteDocument(url,
                JsonUtils.fromURL(new URL(url), getHttpClient()));
        documentCache.put(url, remoteDocument);
        return remoteDocument;
      } catch (final Exception e) {
        throw new JsonLdError(JsonLdError.Error.LOADING_REMOTE_CONTEXT_FAILED, url, e);
      }
    }
  }

  @Override
  public void close() {
    documentCache.clear();
  }
}
