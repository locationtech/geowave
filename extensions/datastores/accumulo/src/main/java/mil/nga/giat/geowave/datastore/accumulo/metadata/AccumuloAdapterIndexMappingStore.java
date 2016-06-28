package mil.nga.giat.geowave.datastore.accumulo.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

/**
 * This class will persist Adapter Index Mappings within an Accumulo table for
 * GeoWave metadata. The mappings will be persisted in an "AIM" column family.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 * 
 * Objects are maintained with regard to visibility. The assumption is that a
 * mapping between an adapter and indexing is consistent across all visibility
 * constraints.
 */
public class AccumuloAdapterIndexMappingStore extends
		AbstractAccumuloPersistence<AdapterToIndexMapping> implements
		AdapterIndexMappingStore
{
	private static final String ADAPTER_INDEX_CF = "AIM";

	public AccumuloAdapterIndexMappingStore(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
	}

	public boolean mappingExists(
			final AdapterToIndexMapping persistedObject ) {
		return objectExists(
				persistedObject.getAdapterId(),
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final AdapterToIndexMapping persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_INDEX_CF;
	}

	@Override
	public AdapterToIndexMapping getIndicesForAdapter(
			ByteArrayId adapterId ) {
		final AdapterToIndexMapping mapping = super.getObject(
				adapterId,
				null,
				null);
		return (mapping != null) ? mapping : new AdapterToIndexMapping(
				adapterId,
				new ByteArrayId[0]);
	}

	@Override
	public void addAdapterIndexMapping(
			AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping {
		if (this.objectExists(
				mapping.getAdapterId(),
				null)) {
			AdapterToIndexMapping oldMapping = super.getObject(
					mapping.getAdapterId(),
					null,
					null);
			if (!oldMapping.equals(mapping)) throw new MismatchedIndexToAdapterMapping(
					oldMapping);
		}
		else {
			addObject(mapping);
		}

	}
}
