package mil.nga.giat.geowave.core.store.metadata;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

/**
 * This abstract class does most of the work for storing persistable objects in
 * Geowave datastores and can be easily extended for any object that needs to be
 * persisted.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 *
 * @param <T>
 *            The type of persistable object that this stores
 */
public abstract class AbstractGeoWavePersistence<T extends Persistable>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWavePersistence.class);

	// TODO: should we concern ourselves with multiple distributed processes
	// updating and looking up objects simultaneously that would require some
	// locking/synchronization mechanism, and even possibly update
	// notifications?
	protected static final int MAX_ENTRIES = 100;
	public final static String METADATA_TABLE = "GEOWAVE_METADATA";
	public final static ByteArrayId METADATA_INDEX_ID = new ByteArrayId(
			METADATA_TABLE);
	protected final DataStoreOperations operations;
	protected final DataStoreOptions options;
	protected final MetadataType type;

	protected final Map<ByteArrayId, T> cache = Collections.synchronizedMap(new LinkedHashMap<ByteArrayId, T>(
			MAX_ENTRIES + 1,
			.75F,
			true) {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean removeEldestEntry(
				final Map.Entry<ByteArrayId, T> eldest ) {
			return size() > MAX_ENTRIES;
		}
	});

	public AbstractGeoWavePersistence(
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final MetadataType type ) {
		this.operations = operations;
		this.options = options;
		this.type = type;
	}

	protected MetadataType getType() {
		return type;
	}

	protected ByteArrayId getSecondaryId(
			final T persistedObject ) {
		// this is the default implementation, if the persistence store requires
		// secondary indices, it needs to override this method
		return null;
	}

	abstract protected ByteArrayId getPrimaryId(
			final T persistedObject );

	public void removeAll() {
		deleteObject(
				null,
				null);
		cache.clear();
	}

	protected ByteArrayId getCombinedId(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// the secondaryId is optional so check for null
		if (secondaryId != null) {
			return new ByteArrayId(
					primaryId.getString() + "_" + secondaryId.getString());
		}
		return primaryId;
	}

	protected void addObjectToCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final T object ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);
		cache.put(
				combinedId,
				object);
	}

	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);
		return cache.get(combinedId);
	}

	protected boolean deleteObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);
		return (cache.remove(combinedId) != null);
	}

	public void remove(
			final ByteArrayId adapterId ) {
		deleteObject(
				adapterId,
				null);

	}

	protected boolean deleteObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		if (deleteObjects(
				primaryId,
				secondaryId,
				authorizations)) {
			deleteObjectFromCache(
					primaryId,
					secondaryId);
			return true;
		}
		return false;
	}

	protected void addObject(
			final T object ) {
		addObject(
				getPrimaryId(object),
				getSecondaryId(object),
				object);
	}

	protected byte[] getVisibility(
			final T entry ) {
		return null;
	}

	protected byte[] toBytes(
			final String s ) {
		if (s == null) {
			return null;
		}
		return s.getBytes(Charset.forName("UTF-8"));
	}

	protected void addObject(
			final ByteArrayId id,
			final ByteArrayId secondaryId,
			final T object ) {
		addObjectToCache(
				id,
				secondaryId,
				object);
		try (final MetadataWriter writer = operations.createMetadataWriter(getType())) {
			if (writer != null) {
				final GeoWaveMetadata metadata = new GeoWaveMetadata(
						id.getBytes(),
						secondaryId != null ? secondaryId.getBytes() : null,
						getVisibility(object),
						getValue(object));
				writer.write(metadata);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to close metadata writer",
					e);
			e.printStackTrace();
		}
	}

	protected byte[] getValue(
			final T object ) {
		return PersistenceUtils.toBinary(object);
	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return internalGetObjects(new MetadataQuery(
				null,
				secondaryId.getBytes(),
				authorizations));
	}

	protected T getObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return internalGetObject(
				primaryId,
				secondaryId,
				true,
				authorizations);
	}

	@SuppressWarnings("unchecked")
	protected T internalGetObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final boolean warnIfNotExists,
			final String... authorizations ) {
		final Object cacheResult = getObjectFromCache(
				primaryId,
				secondaryId);
		if (cacheResult != null) {
			return (T) cacheResult;
		}
		try {
			if (!operations.metadataExists(getType())) {
				if (warnIfNotExists) {
					LOGGER.warn("Object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "' not found. '" + METADATA_TABLE + "' table does not exist");
				}
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
		final MetadataReader reader = operations.createMetadataReader(getType());
		try (final CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(
				primaryId.getBytes(),
				secondaryId == null ? null : secondaryId.getBytes(),
				authorizations))) {
			if (!it.hasNext()) {
				if (warnIfNotExists) {
					LOGGER.warn("Object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "' not found");
				}
				return null;
			}
			final GeoWaveMetadata entry = it.next();
			return entryToValue(entry);
		}
		catch (final IOException e) {
			if (warnIfNotExists) {
				LOGGER.warn(
						"Unable to find object '" + getCombinedId(
								primaryId,
								secondaryId).getString() + "'",
						e);
			}
		}
		return null;
	}

	protected boolean objectExists(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return internalGetObject(
				primaryId,
				secondaryId,
				false,
				authorizations) != null;
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		return internalGetObjects(new MetadataQuery(
				null,
				null,
				authorizations));
	}

	private CloseableIterator<T> internalGetObjects(
			final MetadataQuery query ) {
		try {
			if (!operations.metadataExists(getType())) {
				return new CloseableIterator.Empty<>();
			}
		}
		catch (final IOException e1) {
			LOGGER.error(
					"Unable to check for existence of metadata to get objects",
					e1);
			return new CloseableIterator.Empty<>();
		}
		final MetadataReader reader = operations.createMetadataReader(getType());
		final CloseableIterator<GeoWaveMetadata> it = reader.query(query);
		return new NativeIteratorWrapper(
				it);
	}

	@SuppressWarnings("unchecked")
	protected T fromValue(
			final GeoWaveMetadata entry ) {
		return (T) PersistenceUtils.fromBinary(entry.getValue());
	}

	protected T entryToValue(
			final GeoWaveMetadata entry ) {
		final T result = fromValue(entry);
		if (result != null) {
			addObjectToCache(
					new ByteArrayId(
							entry.getPrimaryId()),
					entry.getSecondaryId() == null ? null : new ByteArrayId(
							entry.getSecondaryId()),
					result);
		}
		return result;
	}

	public boolean deleteObjects(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjects(
				null,
				secondaryId,
				authorizations);
	}

	public boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjects(
				primaryId,
				secondaryId,
				operations,
				getType(),
				this,
				authorizations);
	}

	protected static boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final DataStoreOperations operations,
			final MetadataType type,
			final AbstractGeoWavePersistence cacheDeleter,
			final String... authorizations ) {
		try {
			if (!operations.metadataExists(type)) {
				return false;
			}
		}
		catch (final IOException e1) {
			LOGGER.error(
					"Unable to check for existence of metadata to delete objects",
					e1);
			return false;
		}
		try (final MetadataDeleter deleter = operations.createMetadataDeleter(type)) {
			if (primaryId != null) {
				return deleter.delete(new MetadataQuery(
						primaryId.getBytes(),
						secondaryId != null ? secondaryId.getBytes() : null,
						authorizations));
			}
			boolean retVal = false;
			final MetadataReader reader = operations.createMetadataReader(type);
			try (final CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(
					null,
					secondaryId != null ? secondaryId.getBytes() : null,
					authorizations))) {

				while (it.hasNext()) {
					retVal = true;
					final GeoWaveMetadata entry = it.next();
					if (cacheDeleter != null) {
						cacheDeleter.deleteObjectFromCache(
								new ByteArrayId(
										entry.getPrimaryId()),
								secondaryId);
					}
					deleter.delete(new MetadataQuery(
							entry.getPrimaryId(),
							entry.getSecondaryId(),
							authorizations));
				}
			}
			return retVal;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to delete objects",
					e);
		}
		return false;
	}

	private class NativeIteratorWrapper implements
			CloseableIterator<T>
	{
		final private CloseableIterator<GeoWaveMetadata> it;

		private NativeIteratorWrapper(
				final CloseableIterator<GeoWaveMetadata> it ) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			return entryToValue(it.next());
		}

		@Override
		public void remove() {
			it.remove();
		}

		@Override
		public void close()
				throws IOException {
			it.close();
		}

	}
}