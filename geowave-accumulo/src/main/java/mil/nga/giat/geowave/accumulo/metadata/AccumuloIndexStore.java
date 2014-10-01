package mil.nga.giat.geowave.accumulo.metadata;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The indices will be persisted in an "INDEX" column family.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 * 
 **/
public class AccumuloIndexStore extends
		AbstractAccumuloPersistence<Index> implements
		IndexStore
{
	private static final String INDEX_CF = "INDEX";

	public AccumuloIndexStore(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
	}

	@Override
	public void addIndex(
			final Index index ) {
		addObject(index);
	}

	@Override
	public Index getIndex(
			final ByteArrayId indexId ) {
		return getObject(
				indexId,
				null);
	}

	@Override
	protected String getPersistenceTypeName() {
		return INDEX_CF;
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final Index persistedObject ) {
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
	public CloseableIterator<Index> getIndices() {
		return getObjects();
	}

}
