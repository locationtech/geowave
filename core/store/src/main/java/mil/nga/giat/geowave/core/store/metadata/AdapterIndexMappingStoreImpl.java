package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

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
public class AdapterIndexMappingStoreImpl extends
		AbstractGeoWavePersistence<AdapterToIndexMapping> implements
		AdapterIndexMappingStore
{

	public AdapterIndexMappingStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.AIM);
	}

	public boolean mappingExists(
			final AdapterToIndexMapping persistedObject ) {
		return objectExists(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(persistedObject.getInternalAdapterId())),
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final AdapterToIndexMapping persistedObject ) {
		return new ByteArrayId(
				ByteArrayUtils.shortToByteArray(persistedObject.getInternalAdapterId()));
	}

	@Override
	public AdapterToIndexMapping getIndicesForAdapter(
			final short internalAdapterId ) {

		final AdapterToIndexMapping mapping = super.getObject(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)),
				null,
				null);
		return (mapping != null) ? mapping : new AdapterToIndexMapping(
				internalAdapterId,
				new ByteArrayId[0]);
	}

	@Override
	public void addAdapterIndexMapping(
			final AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping {
		final ByteArrayId internalAdapterId = new ByteArrayId(
				ByteArrayUtils.shortToByteArray(mapping.getInternalAdapterId()));
		if (objectExists(
				internalAdapterId,
				null)) {
			final AdapterToIndexMapping oldMapping = super.getObject(
					internalAdapterId,
					null,
					null);
			if (!oldMapping.equals(mapping)) {
				throw new MismatchedIndexToAdapterMapping(
						oldMapping); // HPFortify FP: accumulo example stores
										// hardcoded password
			}
		}
		else {
			addObject(mapping);
		}

	}

	@Override
	public void remove(
			final short internalAdapterId ) {
		super.remove(new ByteArrayId(
				ByteArrayUtils.shortToByteArray(internalAdapterId)));
	}
}
