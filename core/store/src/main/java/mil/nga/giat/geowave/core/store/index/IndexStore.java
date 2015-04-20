package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * This is responsible for persisting index configuration (either in memory or
 * to disk depending on the implementation).
 */
public interface IndexStore
{
	public void addIndex(
			Index index );

	public Index getIndex(
			ByteArrayId indexId );

	public boolean indexExists(
			ByteArrayId indexId );

	public CloseableIterator<Index> getIndices();
}
