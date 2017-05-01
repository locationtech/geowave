package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
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
	 * @throws IOException
	 */
	public List<ByteArrayId> write(
			final T entry )
			throws IOException;

	public List<ByteArrayId> write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter )
			throws IOException;

	public PrimaryIndex[] getIndices();

	public void flush();
}
