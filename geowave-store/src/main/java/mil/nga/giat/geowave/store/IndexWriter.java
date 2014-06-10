package mil.nga.giat.geowave.store;

import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public interface IndexWriter
{
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			T entry );

	public void close();
}
