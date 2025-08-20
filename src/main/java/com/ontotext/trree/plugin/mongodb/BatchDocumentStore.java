package com.ontotext.trree.plugin.mongodb;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

/**
 * Collects the document data during batch processing.
 *
 * @author <a href="mailto:borislav.bonev@ontotext.com">Borislav Bonev</a>
 * @since 26/03/2025
 */
public class BatchDocumentStore {
	private final MutableLongSet uniqueDocumentIds = LongSets.mutable.empty();
	private final MutableLongList documentIds = LongLists.mutable.empty();
	private final Model data = new LinkedHashModel();

	public void addDocument(long id, Model model) {
		if (uniqueDocumentIds.add(id)) {
			documentIds.add(id);
		}
		data.addAll(model);
	}

	public Model getData() {
		return data;
	}

	public void clear() {
		documentIds.clear();
		uniqueDocumentIds.clear();
		data.clear();
	}

	public int size() {
		return documentIds.size();
	}

	LongIterator getIterator() {
		return documentIds.longIterator();
	}
}
