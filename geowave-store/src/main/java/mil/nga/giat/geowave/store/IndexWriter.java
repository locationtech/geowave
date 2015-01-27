package mil.nga.giat.geowave.store;

import java.io.Closeable;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;

public interface IndexWriter extends
		Closeable
{
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			T entry );

	public <T> void setupAdapter(
			final WritableDataAdapter<T> writableAdapter );

	public Index getIndex();
}
