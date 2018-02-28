package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

/**
 * This class will persist Data Adapters within an Accumulo table for GeoWave
 * metadata. The adapters will be persisted in an "ADAPTER" column family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 */
public class AdapterStoreImpl extends
		AbstractGeoWavePersistence<DataAdapter<?>> implements
		AdapterStore
{

	public AdapterStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.ADAPTER);
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
}
