package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.Writer;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper.ScannerClosableWrapper;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * This abstract class does most of the work for storing persistable objects in
 * Accumulo and can be easily extended for any object that needs to be
 * persisted.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 * 
 * @param <T>
 *            The type of persistable object that this stores
 */
abstract public class AbstractAccumuloPersistence<T extends Persistable>
{
	public final static String METADATA_TABLE = "GEOWAVE_METADATA";
	private final static Logger LOGGER = Logger.getLogger(AbstractAccumuloPersistence.class);
	private final AccumuloOperations accumuloOperations;

	// TODO: should we concern ourselves with multiple distributed processes
	// updating and looking up objects simultaneously that would require some
	// locking/synchronization mechanism, and even possibly update
	// notifications?
	private static final int MAX_ENTRIES = 100;
	protected final Map<ByteArrayId, T> cache = Collections.synchronizedMap(new LinkedHashMap<ByteArrayId, T>(
			MAX_ENTRIES + 1,
			.75F,
			true) {
		/**
				 *
				 */
		private static final long serialVersionUID = 1L;

		// This method is called just after a new entry has been added
		@Override
		public boolean removeEldestEntry(
				final Map.Entry<ByteArrayId, T> eldest ) {
			return size() > MAX_ENTRIES;
		}
	});

	// just attach iterators once per instance
	private boolean iteratorsAttached = false;

	public AbstractAccumuloPersistence(
			final AccumuloOperations accumuloOperations ) {
		this.accumuloOperations = accumuloOperations;
	}

	protected String getAccumuloTablename() {
		return METADATA_TABLE;
	}

	abstract protected String getPersistenceTypeName();

	protected ByteArrayId getSecondaryId(
			final T persistedObject ) {
		// this is the default implementation, if the persistence store requires
		// secondary indices,
		return null;
	}

	abstract protected ByteArrayId getPrimaryId(
			T persistedObject );

	private static Text getSafeText(
			final String text ) {
		if ((text != null) && !text.isEmpty()) {
			return new Text(
					text);
		}
		else {
			return new Text();
		}
	}

	protected String getAccumuloColumnQualifier(
			final T persistedObject ) {
		return getAccumuloColumnQualifier(getSecondaryId(persistedObject));
	}

	protected ByteArrayId getSecondaryId(
			final Key key ) {
		if (key.getColumnQualifier() != null) {
			return new ByteArrayId(
					key.getColumnQualifier().getBytes());
		}
		return null;
	}

	protected ByteArrayId getPrimaryId(
			final Key key ) {
		if (key.getRow() != null) {
			return new ByteArrayId(
					key.getRow().getBytes());
		}
		return null;
	}

	protected ByteArrayId getPeristenceTypeName(
			final Key key ) {
		if (key.getColumnFamily() != null) {
			return new ByteArrayId(
					key.getColumnFamily().getBytes());
		}
		return null;
	}

	protected String getAccumuloColumnQualifier(
			final ByteArrayId secondaryId ) {
		if (secondaryId != null) {
			return secondaryId.getString();
		}
		return null;
	}

	protected byte[] getAccumuloVisibility(
			final T entry ) {
		return null;
	}

	protected String getAccumuloColumnFamily() {
		return getPersistenceTypeName();
	}

	protected ByteArrayId getCombinedId(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// the secondaryId is optional so check for null
		if (secondaryId != null) {
			return new ByteArrayId(
					this.accumuloOperations.getTableNameSpace() + "_" + primaryId.getString() + "_" + secondaryId.getString());
		}
		return primaryId;
	}

	protected void addObject(
			final T object ) {
		final ByteArrayId id = getPrimaryId(object);
		addObjectToCache(object);
		try {

			final Writer writer = accumuloOperations.createWriter(
					getAccumuloTablename(),
					true);
			synchronized (this) {
				if (!iteratorsAttached) {
					iteratorsAttached = true;
					final IteratorConfig[] configs = getIteratorConfig();
					if ((configs != null) && (configs.length > 0)) {
						accumuloOperations.attachIterators(
								getAccumuloTablename(),
								true,
								configs);
					}
				}
			}

			final Mutation mutation = new Mutation(
					new Text(
							id.getBytes()));
			final Text cf = getSafeText(getAccumuloColumnFamily());
			final Text cq = getSafeText(getAccumuloColumnQualifier(object));
			final byte[] visibility = getAccumuloVisibility(object);
			if (visibility != null) {
				mutation.put(
						cf,
						cq,
						new ColumnVisibility(
								visibility),
						new Value(
								PersistenceUtils.toBinary(object)));
			}
			else {
				mutation.put(
						cf,
						cq,
						new Value(
								PersistenceUtils.toBinary(object)));
			}
			writer.write(mutation);
			writer.close();
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable add object",
					e);
		}
	}

	protected void addObjectToCache(
			final T object ) {
		final ByteArrayId combinedId = getCombinedId(
				getPrimaryId(object),
				getSecondaryId(object));
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

	protected IteratorConfig[] getIteratorConfig() {
		return null;
	}

	protected IteratorSetting[] getScanSettings() {
		return null;
	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		try {
			final BatchScanner scanner = getScanner(
					null,
					secondaryId,
					authorizations);
			final Iterator<Entry<Key, Value>> it = scanner.iterator();
			return new CloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							scanner),
					new NativeIteratorWrapper(
							it));
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to find objects, table '" + getAccumuloTablename() + "' does not exist",
					e);
		}
		return new CloseableIterator.Empty<T>();
	}

	@SuppressWarnings("unchecked")
	protected T getObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final Object cacheResult = getObjectFromCache(
				primaryId,
				secondaryId);
		if (cacheResult != null) {
			return (T) cacheResult;
		}
		try {
			final BatchScanner scanner = getScanner(
					primaryId,
					secondaryId,
					authorizations);
			try {
				final Iterator<Entry<Key, Value>> it = scanner.iterator();
				if (!it.hasNext()) {
					LOGGER.warn("Object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "' not found");
					return null;
				}
				final Entry<Key, Value> entry = it.next();
				return entryToValue(entry);
			}
			finally {
				scanner.close();
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to find object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "'",
					e);
		}
		return null;
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		try {
			final BatchScanner scanner = getFullScanner(authorizations);
			final Iterator<Entry<Key, Value>> it = scanner.iterator();
			return new CloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							scanner),
					new NativeIteratorWrapper(
							it));
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to find objects, table '" + getAccumuloTablename() + "' does not exist",
					e);
		}
		return new CloseableIterator.Empty<T>();
	}

	@SuppressWarnings("unchecked")
	protected T entryToValue(
			final Entry<Key, Value> entry ) {
		final T result = (T) PersistenceUtils.fromBinary(
				entry.getValue().get(),
				Persistable.class);
		if (result != null) {
			addObjectToCache(result);
		}
		return result;
	}

	private BatchScanner getFullScanner(
			final String... authorizations )
			throws TableNotFoundException {
		return getScanner(
				null,
				null,
				authorizations);
	}

	protected BatchScanner getScanner(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations )
			throws TableNotFoundException {
		final BatchScanner scanner = accumuloOperations.createBatchScanner(
				getAccumuloTablename(),
				authorizations);
		final IteratorSetting[] settings = getScanSettings();
		if ((settings != null) && (settings.length > 0)) {
			for (final IteratorSetting setting : settings) {
				scanner.addScanIterator(setting);
			}
		}
		final String columnFamily = getAccumuloColumnFamily();
		final String columnQualifier = getAccumuloColumnQualifier(secondaryId);
		if (columnFamily != null) {
			if (columnQualifier != null) {
				scanner.fetchColumn(
						new Text(
								columnFamily),
						new Text(
								columnQualifier));
			}
			else {
				scanner.fetchColumnFamily(new Text(
						columnFamily));
			}
		}
		final Collection<Range> ranges = new ArrayList<Range>();
		if (primaryId != null) {
			ranges.add(new Range(
					new Text(
							primaryId.getBytes())));
		}
		else {
			ranges.add(new Range());
		}
		scanner.setRanges(ranges);
		return scanner;
	}

	protected boolean deleteObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjectFromCache(
				primaryId,
				secondaryId) && accumuloOperations.delete(
				getAccumuloTablename(),
				Arrays.asList(primaryId),
				getAccumuloColumnFamily(),
				getAccumuloColumnQualifier(secondaryId),
				authorizations);
	}

	public boolean deleteObjects(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		try {
			final BatchScanner scanner = getScanner(
					null,
					secondaryId,
					authorizations);
			final Iterator<Entry<Key, Value>> it = scanner.iterator();
			try (final CloseableIterator<?> cit = new CloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							scanner),
					new DeleteIteratorWrapper(
							it,
							authorizations))) {
				while (cit.hasNext()) {
					deleteObjectFromCache(
							getPrimaryId((T) cit.next()),
							secondaryId);
				}
			}
			catch (IOException e) {
				LOGGER.error(
						"Unable to delete objects",
						e);
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to find objects, table '" + getAccumuloTablename() + "' does not exist",
					e);
		}

		return true;
	}

	protected boolean objectExists(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		if (getObjectFromCache(
				primaryId,
				secondaryId) != null) {
			return true;
		}
		try {
			final BatchScanner scanner = getScanner(
					primaryId,
					secondaryId);

			try {
				final Iterator<Entry<Key, Value>> it = scanner.iterator();
				if (it.hasNext()) {
					// may as well cache the result
					return (entryToValue(it.next()) != null);
				}
				else {
					return false;
				}

			}
			finally {
				scanner.close();
			}
		}
		catch (final TableNotFoundException e) {
			// this is only debug, because if the table doesn't exist, its
			// essentially empty, but doesn't necessarily indicate an issue
			LOGGER.debug(
					"Unable to check existence of object '" + getCombinedId(
							primaryId,
							secondaryId) + "'",
					e);
		}
		return false;
	}

	private class DeleteIteratorWrapper implements
			Iterator<T>
	{
		String[] authorizations;
		final private Iterator<Entry<Key, Value>> it;

		private DeleteIteratorWrapper(
				final Iterator<Entry<Key, Value>> it,
				String[] authorizations ) {
			this.it = it;
			this.authorizations = authorizations;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			Map.Entry<Key, Value> entry = it.next();
			accumuloOperations.delete(
					getAccumuloTablename(),
					Arrays.asList(new ByteArrayId(
							entry.getKey().getRowData().getBackingArray())),
					entry.getKey().getColumnFamily().toString(),
					entry.getKey().getColumnQualifier().toString(),
					authorizations);
			return entryToValue(entry);
		}

		@Override
		public void remove() {
			it.remove();
		}

	}

	private class NativeIteratorWrapper implements
			Iterator<T>
	{
		final private Iterator<Entry<Key, Value>> it;

		private NativeIteratorWrapper(
				final Iterator<Entry<Key, Value>> it ) {
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

	}

}
