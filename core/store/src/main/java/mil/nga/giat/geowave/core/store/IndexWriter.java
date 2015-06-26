package mil.nga.giat.geowave.core.store;

import java.io.Closeable;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;

public interface IndexWriter extends
		Closeable
{
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			T entry );

	public <T> void setupAdapter(
			final WritableDataAdapter<T> writableAdapter );

	public Index getIndex();

	public void flush();
}
