package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
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
	private final Map<ByteArrayId, T> cache = Collections.synchronizedMap(new LinkedHashMap<ByteArrayId, T>(
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

	public AbstractAccumuloPersistence(
			final AccumuloOperations accumuloOperations ) {
		this.accumuloOperations = accumuloOperations;
	}

	protected String getAccumuloTablename() {
		return METADATA_TABLE;
	}

	abstract protected String getAccumuloColumnFamilyName();

	abstract protected ByteArrayId getId(
			T persistedObject );

	protected void addObject(
			final T object ) {
		final ByteArrayId id = getId(object);
		cache.put(
				id,
				object);
		try {

			final Writer writer = accumuloOperations.createWriter(getAccumuloTablename());
			final Mutation mutation = new Mutation(
					new Text(
							id.getBytes()));
			mutation.put(
					new Text(
							getAccumuloColumnFamilyName()),
					new Text(),
					new Value(
							PersistenceUtils.toBinary(object)));

			writer.write(mutation);
			writer.close();
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable add object",
					e);
		}
	}

	@SuppressWarnings("unchecked")
	protected T getObject(
			final ByteArrayId id ) {
		final Object cacheResult = cache.get(id);
		if (cacheResult != null) {
			return (T) cacheResult;
		}
		try {
			final BatchScanner scanner = getScanner(id);
			try {
				final Iterator<Entry<Key, Value>> it = scanner.iterator();
				if (!it.hasNext()) {
					LOGGER.warn("Object '" + StringUtils.stringFromBinary(id.getBytes()) + "' not found");
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
					"Unable to find object '" + StringUtils.stringFromBinary(id.getBytes()) + "'",
					e);
		}
		return null;
	}

	protected Iterator<T> getObjects() {
		try {
			final BatchScanner scanner = getFullScanner();
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
		return null;
	}

	@SuppressWarnings("unchecked")
	private T entryToValue(
			final Entry<Key, Value> entry ) {
		final T result = (T) PersistenceUtils.fromBinary(
				entry.getValue().get(),
				Persistable.class);
		if (result != null) {
			cache.put(
					getId(result),
					result);
		}
		return result;
	}

	private BatchScanner getFullScanner()
			throws TableNotFoundException {
		final BatchScanner scanner = accumuloOperations.createBatchScanner(getAccumuloTablename());
		scanner.fetchColumnFamily(new Text(
				getAccumuloColumnFamilyName()));
		final Collection<Range> ranges = new ArrayList<Range>();
		ranges.add(new Range());
		scanner.setRanges(ranges);
		return scanner;
	}

	private BatchScanner getScanner(
			final ByteArrayId id )
			throws TableNotFoundException {
		final BatchScanner scanner = accumuloOperations.createBatchScanner(getAccumuloTablename());
		scanner.fetchColumnFamily(new Text(
				getAccumuloColumnFamilyName()));
		final Collection<Range> ranges = new ArrayList<Range>();
		ranges.add(new Range(
				new Text(
						id.getBytes())));
		scanner.setRanges(ranges);
		return scanner;
	}

	protected boolean objectExists(
			final ByteArrayId id ) {
		if (cache.get(id) != null) {
			return true;
		}
		try {
			final BatchScanner scanner = getScanner(id);

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
					"Unable to check existence of data adapter '" + StringUtils.stringFromBinary(id.getBytes()) + "'",
					e);
		}
		return false;
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
