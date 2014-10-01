package mil.nga.giat.geowave.accumulo.metadata;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

/**
 * This class will persist Data Adapters within an Accumulo table for GeoWave
 * metadata. The adapters will be persisted in an "ADAPTER" column family.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 */
public class AccumuloAdapterStore extends
		AbstractAccumuloPersistence<DataAdapter<?>> implements
		AdapterStore
{
	private static final String ADAPTER_CF = "ADAPTER";

	public AccumuloAdapterStore(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
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
	protected ByteArrayId getPrimaryId(
			final DataAdapter<?> persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_CF;
	}
}
