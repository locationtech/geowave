package mil.nga.giat.geowave.store;

import java.io.Closeable;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public interface IndexWriter extends
		Closeable
{
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			T entry );
}
