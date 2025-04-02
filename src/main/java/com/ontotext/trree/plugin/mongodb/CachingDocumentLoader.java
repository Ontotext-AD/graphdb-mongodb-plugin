package com.ontotext.trree.plugin.mongodb;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdErrorCode;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.DocumentLoaderOptions;
import com.apicatalog.jsonld.loader.SchemeRouter;

import java.io.Closeable;
import java.net.URI;
import java.util.*;

/**
 * Custom document loader that will cache the remote documents. This will not cache regular JSON-LD
 * documents but rather only remote {@code "@contexts": "http://remote.context.file"}
 *
 * @author <a href="mailto:borislav.bonev@ontotext.com">Borislav Bonev</a>
 * @since 19/10/2022
 */
public class CachingDocumentLoader implements DocumentLoader, Closeable {
  private static final DocumentLoader defaultLoader = SchemeRouter.defaultInstance();
  private static final String DISALLOW_REMOTE_CONTEXT_LOADING = "graphdb.disallow.remote.context.loading";

  // as the document instances are immutable we can cache them whole
  private final Map<URI, Document> documentCache = new LinkedHashMap<URI, Document>() {
    @Override protected boolean removeEldestEntry(Map.Entry eldest) {
      // if the cache reaches 1000 elements then the first (oldest) entity will be dropped from the cache
      // this is mainly a failsafe in case of someone tries to fill up the memory with random contexts
      return documentCache.size() > 1000;
    }
  };

  @Override
  public Document loadDocument(URI uri, DocumentLoaderOptions options) throws JsonLdError {
    if (documentCache.containsKey(uri)) {
      try {
        return documentCache.get(uri);
      } catch (final Exception e) {
        throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, uri + " " + e.getMessage());
      }
    } else {
      final String disallowRemote = System
              .getProperty(DISALLOW_REMOTE_CONTEXT_LOADING);
      if ("true".equalsIgnoreCase(disallowRemote)) {
        throw new JsonLdError(JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED,
                "Remote context loading has been disallowed (uri was " + uri + ")");
      }

      try {
        Document remoteDocument = defaultLoader.loadDocument(uri, options);
        documentCache.put(uri, remoteDocument);
        return remoteDocument;
      } catch (final Exception e) {
        throw new JsonLdError(JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED, uri + " " + e.getMessage());
      }
    }
  }

  @Override
  public void close() {
    documentCache.clear();
  }
}
