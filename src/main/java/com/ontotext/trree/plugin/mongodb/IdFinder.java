package com.ontotext.trree.plugin.mongodb;
import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.document.JsonDocument;
import jakarta.json.*;
import jakarta.json.stream.JsonParser;

import static com.apicatalog.jsonld.lang.Keywords.GRAPH;
import static com.apicatalog.jsonld.lang.Keywords.ID;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class IdFinder {
	private MongoDBPlugin plugin;

    public IdFinder(MongoDBPlugin plugin) {
        this.plugin = plugin;
    }

    private static String getStringValue(Object obj) {
        if (obj == null) return null;
        if (obj instanceof JsonString jsonString) {
           return jsonString.getString();
        }
        return obj.toString();
    }

	public String extractRootUri(Map<String, ? extends Object> doc) {
		String uri = null;
		if (doc.containsKey(GRAPH)) {
			Object item = doc.get(GRAPH);
			if (item != null) {
				if (item instanceof List<?> listItem) {
					if (!listItem.isEmpty() && listItem.getFirst() instanceof Map) {
						@SuppressWarnings("unchecked")
                        Map<String, Object> graphDoc = (Map<String, Object>) listItem.getFirst();
						uri = getStringValue(graphDoc.get(ID));
						if (listItem.size() > 1) {
							plugin.getLogger().warn("Multiple graphs in mongo document. Selecting the first one for entity: " + uri);
						}
					} else {
						plugin.getLogger().warn("Value of @graph must be a valid document in mongo document.");
					}
				} else if (item instanceof Map) {
					@SuppressWarnings("unchecked")
					Map<String, Object> graphDoc = (Map<String, Object>) item;
					uri = getStringValue(graphDoc.get(ID));
				} else {
					plugin.getLogger().warn("@graph must be a document or list of documents in mongo document.");
				}
			}
		}
		if (uri == null) {
			uri = getStringValue(doc.get(ID));
		}
		return uri;
	}

    // Depth 3 is usually safe for very complex nesting (like BSON inside a @nest container)
    // Depth 1 = Root keys
    // Depth 2 = _id -> $oid
    private static final int MAX_DEPTH = 3;

    public String extractRootUri(String json) {
        try (StringReader reader = new StringReader(json);
             JsonParser parser = Json.createParser(reader)) {
            
            // Advance to the first token (START_OBJECT)
            if (!parser.hasNext()) {
                return null;
            }
            parser.next();
            
            JsonObject root = parser.getObject();

            // Create a "pruned" copy of the document
            // This keeps @context and top-level structure, but cuts off deep trees.
            JsonObject prunedRoot = (JsonObject) pruneTree(root, 0);

            // Expand the Pruned Document
            JsonArray expanded = JsonLd.expand(JsonDocument.of(prunedRoot)).get();
            System.out.println("Expanded JSON " + expanded.toString());

            if (expanded.isEmpty()) return null;

            // The first item in the expanded list corresponds to the input root object
            JsonObject mainNode = expanded.getJsonObject(0);

            // if (mainNode.containsKey("@id")) {
            //     return mainNode.getString("@id");
            // }
            return extractRootUri(mainNode);

            //return null; 

        } catch (Exception e) {
			plugin.getLogger().warn("Exception while trying to find root ID by expanding the JSON-LD document", e);
            throw new RuntimeException("Failed to find root ID", e);
        }
    }


    /**
     * Recursively copies a JSON tree but stops at MAX_DEPTH.
     */
    private JsonValue pruneTree(JsonValue value, int currentDepth) {
        if (value == null) return JsonValue.NULL;

        // Always keep Primitives (Strings, Numbers, Booleans)
        if (isPrimitive(value)) {
            return value;
        }

        // If we hit the depth limit, return NULL (effectively pruning this branch)
        if (currentDepth >= MAX_DEPTH) {
            return JsonValue.NULL;
        }

        // Handle Objects
        if (value.getValueType() == JsonValue.ValueType.OBJECT) {
            JsonObject obj = value.asJsonObject();
            JsonObjectBuilder builder = Json.createObjectBuilder();

            for (Map.Entry<String, JsonValue> entry : obj.entrySet()) {
                // Always preserve @context regardless of depth
                if (entry.getKey().equals("@context")) {
                    builder.add(entry.getKey(), entry.getValue());
                } else {
                    // Recurse
                    JsonValue child = pruneTree(entry.getValue(), currentDepth + 1);
                    if (child != JsonValue.NULL) {
                        builder.add(entry.getKey(), child);
                    }
                }
            }
            return builder.build();
        }

        // Handle Arrays 
        // (Optionally: you could skip arrays entirely if you know IDs are never in lists)
        if (value.getValueType() == JsonValue.ValueType.ARRAY) {
            JsonArray arr = value.asJsonArray();
            JsonArrayBuilder builder = Json.createArrayBuilder();
            
            // Optimization: Only keep the first element of arrays to detect ID, 
            // ignore the rest to save performance.
            if (!arr.isEmpty()) {
                 JsonValue child = pruneTree(arr.get(0), currentDepth + 1);
                 if (child != JsonValue.NULL) builder.add(child);
            }
            return builder.build();
        }

        return JsonValue.NULL;
    }

    private static boolean isPrimitive(JsonValue value) {
        JsonValue.ValueType type = value.getValueType();
        return type == JsonValue.ValueType.STRING ||
               type == JsonValue.ValueType.NUMBER ||
               type == JsonValue.ValueType.TRUE ||
               type == JsonValue.ValueType.FALSE;
    }
    
}
