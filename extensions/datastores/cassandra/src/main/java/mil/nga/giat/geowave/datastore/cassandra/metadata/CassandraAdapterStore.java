package mil.nga.giat.geowave.datastore.cassandra.metadata;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraAdapterStore extends
		AbstractCassandraPersistence<DataAdapter<?>> implements
		AdapterStore
{
	private final static Logger LOGGER = Logger.getLogger(CassandraAdapterStore.class);
	protected static final String ADAPTER_CF = "ADAPTER";

	public CassandraAdapterStore(
			final CassandraOperations operations ) {
		super(
				operations);
	}

	@Override
	public void addAdapter(
			final DataAdapter<?> adapter ) {
		addObject(adapter);

	}

	@Override
	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		return getObject(
				adapterId,
				null);
	}

	@Override
	public boolean adapterExists(
			final ByteArrayId adapterId ) {
		return objectExists(
				adapterId,
				null);
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final DataAdapter<?> persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_CF;
	}

}
