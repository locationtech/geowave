package mil.nga.giat.geowave.core.store.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * This is a simple HashMap based in-memory implementation of the IndexStore and
 * can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an accumulo table.
 */
public class MemoryIndexStore implements
		IndexStore
{
	private final Map<ByteArrayId, Index> indexMap = new HashMap<ByteArrayId, Index>();

	public MemoryIndexStore(
			final Index[] initialIndices ) {
		for (final Index index : initialIndices) {
			addIndex(index);
		}
	}

	@Override
	public void addIndex(
			final Index index ) {
		indexMap.put(
				index.getId(),
				index);
	}

	@Override
	public Index getIndex(
			final ByteArrayId indexId ) {
		return indexMap.get(indexId);
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId ) {
		return indexMap.containsKey(indexId);
	}

	@Override
	public CloseableIterator<Index> getIndices() {
		return new CloseableIterator.Wrapper<Index>(
				new ArrayList<Index>(
						indexMap.values()).iterator());
	}

}
