package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;

public interface IndexWriter extends
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
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry );

	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter );

	public <T> void setupAdapter(
			final WritableDataAdapter<T> writableAdapter );

	public PrimaryIndex getIndex();

	public void flush();
}
