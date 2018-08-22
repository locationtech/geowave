package mil.nga.giat.geowave.core.store.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
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
		AbstractGeoWavePersistence<InternalDataAdapter<?>> implements
		PersistentAdapterStore
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AdapterStoreImpl.class);

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
			final InternalDataAdapter<?> adapter ) {
		addObject(adapter);
	}

	@Override
	public InternalDataAdapter<?> getAdapter(
			final Short internalAdapterId ) {
		if (internalAdapterId == null) {
			LOGGER.warn("Cannot get adapter for null internal ID");
			return null;
		}
		return getObject(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)),
				null);
	}

	@Override
	protected InternalDataAdapter<?> fromValue(
			final GeoWaveMetadata entry ) {
		final WritableDataAdapter<?> adapter = (WritableDataAdapter<?>) PersistenceUtils.fromBinary(entry.getValue());
		return new InternalDataAdapterWrapper<>(
				adapter,
				ByteArrayUtils.byteArrayToShort(entry.getPrimaryId()));
	}

	@Override
	protected byte[] getValue(
			final InternalDataAdapter<?> object ) {
		return PersistenceUtils.toBinary(object.getAdapter());
	}

	@Override
	public boolean adapterExists(
			final Short internalAdapterId ) {
		if (internalAdapterId == null) {
			LOGGER.warn("Cannot check existence of adapter for null internal ID");
			return false;
		}
		return objectExists(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)),
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final InternalDataAdapter<?> persistedObject ) {
		return new ByteArrayId(
				ByteArrayUtils.shortToByteArray(persistedObject.getInternalAdapterId()));
	}

	@Override
	public CloseableIterator<InternalDataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	public void removeAdapter(
			final Short internalAdapterId ) {
		if (internalAdapterId == null) {
			LOGGER.warn("Cannot remove adapter for null internal ID");
			return;
		}
		remove(new ByteArrayId(
				ByteArrayUtils.shortToByteArray(internalAdapterId)));
	}
}
