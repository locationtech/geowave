package mil.nga.giat.geowave.accumulo;

import java.util.Iterator;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The adapters will be persisted in an "INDEX" column family.
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
		return getObject(indexId);
	}

	@Override
	protected String getAccumuloColumnFamilyName() {
		return INDEX_CF;
	}

	@Override
	protected ByteArrayId getId(
			final Index persistedObject ) {
		return persistedObject.getId();
	}

	@Override
	public boolean indexExists(
			final ByteArrayId id ) {
		return objectExists(id);
	}

	@Override
	public Iterator<Index> getIndices() {
		return getObjects();
	}

}
