package mil.nga.giat.geowave.core.store.metadata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

/**
 * This class will persist Adapter Internal Adapter Mappings within an Accumulo
 * table for GeoWave metadata. The mappings will be persisted in an "AIM" column
 * family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 *
 * Objects are maintained with regard to visibility. The assumption is that a
 * mapping between an adapter and indexing is consistent across all visibility
 * constraints.
 */
public class InternalAdapterStoreImpl implements
		InternalAdapterStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(InternalAdapterStoreImpl.class);
	private static final Object MUTEX = new Object();
	protected final BiMap<ByteArrayId, Short> cache = HashBiMap.create();
	private static final byte[] INTERNAL_TO_EXTERNAL_ID = new byte[] {
		0
	};
	private static final byte[] EXTERNAL_TO_INTERNAL_ID = new byte[] {
		1
	};

	private static final ByteArrayId INTERNAL_TO_EXTERNAL_BYTEARRAYID = new ByteArrayId(
			INTERNAL_TO_EXTERNAL_ID);
	private static final ByteArrayId EXTERNAL_TO_INTERNAL_BYTEARRAYID = new ByteArrayId(
			EXTERNAL_TO_INTERNAL_ID);
	private final DataStoreOperations operations;

	public InternalAdapterStoreImpl(
			final DataStoreOperations operations ) {
		this.operations = operations;
	}

	private MetadataReader getReader(
			final boolean warnIfNotExists ) {
		try {
			if (!operations.metadataExists(MetadataType.INTERNAL_ADAPTER)) {
				return null;
			}
		}
		catch (final IOException e1) {
			if (warnIfNotExists) {
				LOGGER.error(
						"Unable to check for existence of metadata to get object",
						e1);
			}
			return null;
		}
		return operations.createMetadataReader(MetadataType.INTERNAL_ADAPTER);
	}

	@Override
	public ByteArrayId getAdapterId(
			final short internalAdapterId ) {
		return internalGetAdapterId(
				internalAdapterId,
				true);
	}

	private ByteArrayId internalGetAdapterId(
			final short internalAdapterId,
			final boolean warnIfNotExists ) {
		final ByteArrayId id = cache.inverse().get(
				internalAdapterId);
		if (id != null) {
			return id;
		}
		final MetadataReader reader = getReader(true);
		if (reader == null) {
			if (warnIfNotExists) {
				LOGGER.warn("Internal Adapter ID '" + internalAdapterId + "' not found. '"
						+ AbstractGeoWavePersistence.METADATA_TABLE + "' table does not exist");
			}
			return null;
		}
		try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(
				ByteArrayUtils.shortToByteArray(internalAdapterId),
				INTERNAL_TO_EXTERNAL_ID))) {
			if (!it.hasNext()) {
				if (warnIfNotExists) {
					LOGGER.warn("Internal Adapter ID '" + internalAdapterId + "' not found");
				}
				return null;
			}
			final ByteArrayId adapterId = new ByteArrayId(
					it.next().getValue());
			cache.put(
					adapterId,
					internalAdapterId);
			return adapterId;
		}
		catch (final IOException e) {
			if (warnIfNotExists) {
				LOGGER.warn(
						"Unable to find Internal Adapter ID '" + internalAdapterId + "'",
						e);
			}
		}
		return null;
	}

	@Override
	public Short getInternalAdapterId(
			final ByteArrayId adapterId ) {
		return internalGetInternalAdapterId(
				adapterId,
				true);
	}

	public Short internalGetInternalAdapterId(
			final ByteArrayId adapterId,
			final boolean warnIfNotExist ) {
		final Short id = cache.get(adapterId);
		if (id != null) {
			return id;
		}

		final MetadataReader reader = getReader(warnIfNotExist);
		if (reader == null) {
			if (warnIfNotExist) {
				LOGGER.warn("Adapter '" + adapterId.getString() + "' not found. '"
						+ AbstractGeoWavePersistence.METADATA_TABLE + "' table does not exist");

			}
			return null;
		}
		try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(
				adapterId.getBytes(),
				EXTERNAL_TO_INTERNAL_ID))) {
			if (!it.hasNext()) {
				if (warnIfNotExist) {
					LOGGER.warn("Adapter '" + adapterId.getString() + "' not found");
				}
				return null;
			}
			final short internalAdapterId = ByteArrayUtils.byteArrayToShort(it.next().getValue());
			cache.put(
					adapterId,
					internalAdapterId);
			return internalAdapterId;
		}
		catch (final IOException e) {
			if (warnIfNotExist) {
				LOGGER.warn(
						"Unable to find adapter '" + adapterId.getString() + "'",
						e);
			}
		}
		return null;
	}

	public static short getInitialInternalAdapterId(
			final ByteArrayId adapterId ) {
		final int shortRange = Short.MAX_VALUE - Short.MIN_VALUE;
		final short internalAdapterId = (short) (Math.abs((adapterId.hashCode() % shortRange)) - Short.MIN_VALUE);
		return internalAdapterId;
	}

	private boolean internalAdapterIdExists(
			final short internalAdapterId ) {
		return internalGetAdapterId(
				internalAdapterId,
				false) != null;
	}

	// ** this introduces a distributed race condition if multiple JVM processes
	// are excuting this method simulatneously
	// care should be taken to either explicitly call this from a single client
	// before running a distributed job, or use a distributed locking mechanism
	// so that internal Adapter Ids are consistent without any race conditions
	@Override
	public short addAdapterId(
			final ByteArrayId adapterId ) {
		synchronized (MUTEX) {
			Short internalAdapterId = internalGetInternalAdapterId(
					adapterId,
					false);
			if (internalAdapterId != null) {
				return internalAdapterId;
			}
			internalAdapterId = getInitialInternalAdapterId(adapterId);
			while (internalAdapterIdExists(internalAdapterId)) {
				internalAdapterId++;
			}
			try (final MetadataWriter writer = operations.createMetadataWriter(MetadataType.INTERNAL_ADAPTER)) {
				if (writer != null) {
					final byte[] internalAdapterIdBytes = ByteArrayUtils.shortToByteArray(internalAdapterId);
					writer.write(new GeoWaveMetadata(
							adapterId.getBytes(),
							EXTERNAL_TO_INTERNAL_ID,
							null,
							internalAdapterIdBytes));
					writer.write(new GeoWaveMetadata(
							internalAdapterIdBytes,
							INTERNAL_TO_EXTERNAL_ID,
							null,
							adapterId.getBytes()));
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close metadata writer",
						e);
				e.printStackTrace();
			}
			return internalAdapterId;
		}
	}

	@Override
	public boolean remove(
			final ByteArrayId adapterId ) {
		final Short internalAdapterId = getInternalAdapterId(adapterId);
		return delete(
				adapterId,
				internalAdapterId);
	}

	private boolean delete(
			final ByteArrayId adapterId,
			final Short internalAdapterId ) {
		boolean externalDeleted = false;
		if (adapterId != null) {
			externalDeleted = AbstractGeoWavePersistence.deleteObjects(
					adapterId,
					EXTERNAL_TO_INTERNAL_BYTEARRAYID,
					operations,
					MetadataType.INTERNAL_ADAPTER,
					null);
			cache.remove(adapterId);
		}
		boolean internalDeleted = false;
		if (internalAdapterId != null) {
			internalDeleted = AbstractGeoWavePersistence.deleteObjects(
					new ByteArrayId(
							ByteArrayUtils.shortToByteArray(internalAdapterId)),
					INTERNAL_TO_EXTERNAL_BYTEARRAYID,
					operations,
					MetadataType.INTERNAL_ADAPTER,
					null);
		}
		return internalDeleted && externalDeleted;
	}

	@Override
	public void removeAll() {
		AbstractGeoWavePersistence.deleteObjects(
				null,
				null,
				operations,
				MetadataType.INTERNAL_ADAPTER,
				null);
		cache.clear();
	}

	@Override
	public boolean remove(
			final short internalAdapterId ) {
		final ByteArrayId adapterId = getAdapterId(internalAdapterId);
		return delete(
				adapterId,
				internalAdapterId);
	}
}
