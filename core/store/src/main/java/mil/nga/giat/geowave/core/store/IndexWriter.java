package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexWriter<T> extends
		Closeable
{
	/**
	 * Write the entry using the index writer's configure field visibility
	 * writer.
	 * 
	 * @param writableAdapter
	 * @param entry
	 * @return
	 */
	public InsertionIds write(
			final T entry );

	public InsertionIds write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter );

	public PrimaryIndex[] getIndices();

	public void flush();
}
