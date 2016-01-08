package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

public interface IndexStore
{
	public void addIndex(
			Index<?, ?> index );

	public Index<?, ?> getIndex(
			ByteArrayId indexId );

	public boolean indexExists(
			ByteArrayId indexId );

	public CloseableIterator<Index<?, ?>> getIndices();
}
