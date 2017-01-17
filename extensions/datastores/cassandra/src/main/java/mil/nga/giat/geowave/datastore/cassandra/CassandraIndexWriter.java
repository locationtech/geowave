package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraIndexWriter<T> extends
		DataStoreIndexWriter<T, CassandraRow>
{
	protected final CassandraOperations operations;
	protected final CassandraDataStore dataStore;

	public CassandraIndexWriter(
			final CassandraDataStore dataStore,
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final CassandraOperations operations,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		super(
				adapter,
				index,
				null,
				null,
				callback,
				closable);
		this.dataStore = dataStore;
		this.operations = operations;
	}

	@Override
	protected void ensureOpen() {
		if (writer == null) {
			writer = operations.createWriter(
					index.getId().getString(),
					true);
		}
	}

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		return dataStore.write(
				(WritableDataAdapter<T>) adapter,
				index,
				entry,
				writer,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
	}

}
