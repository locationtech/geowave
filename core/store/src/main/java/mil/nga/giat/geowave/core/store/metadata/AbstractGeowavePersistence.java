package mil.nga.giat.geowave.core.store.metadata;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.DataStoreOperations;

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
public abstract class AbstractGeowavePersistence<T extends Persistable>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractGeowavePersistence.class);

	// TODO: should we concern ourselves with multiple distributed processes
	// updating and looking up objects simultaneously that would require some
	// locking/synchronization mechanism, and even possibly update
	// notifications?
	protected static final int MAX_ENTRIES = 100;
	public final static String METADATA_TABLE = "GEOWAVE_METADATA";
	private final DataStoreOperations operations;

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

	public AbstractGeowavePersistence(
			final DataStoreOperations operations ) {
		this.operations = operations;
	}

	abstract protected String getPersistenceTypeName();

	protected ByteArrayId getSecondaryId(
			final T persistedObject ) {
		// this is the default implementation, if the persistence store requires
		// secondary indices,
		return null;
	}

	abstract protected ByteArrayId getPrimaryId(
			final T persistedObject );

	protected String getTablename() {
		return METADATA_TABLE;
	}

	public void removeAll() {
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
		return deleteObjectFromCache(
				primaryId,
				secondaryId) && deleteObjects(
				primaryId,
				secondaryId,
				authorizations);
	}

	protected void addObject(
			final T object ) {
		addObject(
				getPrimaryId(object),
				getSecondaryId(object),
				object);
	}

	protected String getColumnQualifier(
			final ByteArrayId secondaryId ) {
		if (secondaryId != null) {
			return secondaryId.getString();
		}
		return null;
	}

	protected String getColumnQualifier(
			final T persistedObject ) {
		return getColumnQualifier(getSecondaryId(persistedObject));
	}

	protected byte[] getVisibility(
			final T entry ) {
		return null;
	}

	protected String getColumnFamily() {
		return getPersistenceTypeName();
	}

	protected abstract void addObject(
			final ByteArrayId id,
			final ByteArrayId secondaryId,
			final T object );

	protected abstract boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations );

	protected byte[] toBytes(
			final String s ) {
		if (s == null) {
			return null;
		}
		return s.getBytes(Charset.forName("UTF-8"));
	}

}
