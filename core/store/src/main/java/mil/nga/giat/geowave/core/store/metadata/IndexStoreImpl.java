package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The indices will be persisted in an "INDEX" column family.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 * 
 **/
public class IndexStoreImpl extends
		AbstractGeoWavePersistence<Index<?, ?>> implements
		IndexStore
{
	public IndexStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.INDEX);
	}

	@Override
	public void addIndex(
			final Index<?, ?> index ) {
		addObject(index);
	}

	@Override
	public Index<?, ?> getIndex(
			final ByteArrayId indexId ) {
		return getObject(
				indexId,
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final Index<?, ?> persistedObject ) {
		return persistedObject.getId();
	}

	@Override
	public boolean indexExists(
			final ByteArrayId id ) {
		return objectExists(
				id,
				null);
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices() {
		return getObjects();
	}

}
