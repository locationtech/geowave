package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;

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
abstract public class AbstractAccumuloPersistence<T extends Persistable> extends
		AbstractGeowavePersistence<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloPersistence.class);
	protected final AccumuloOperations accumuloOperations;
	protected Text row = new Text();

	// just attach iterators once per instance
	private boolean iteratorsAttached = false;

	public AbstractAccumuloPersistence(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
		this.accumuloOperations = accumuloOperations;
	}

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
		return new ByteArrayId(
				key.getRow().getBytes());
	}

	protected ByteArrayId getPeristenceTypeName(
			final Key key ) {
		if (key.getColumnFamily() != null) {
			return new ByteArrayId(
					key.getColumnFamily().getBytes());
		}
		return null;
	}

	@Override
	protected void addObject(
			final ByteArrayId id,
			final ByteArrayId secondaryId,
			final T object ) {
		addObjectToCache(
				id,
				secondaryId,
				object);

		try {

			final Writer writer = accumuloOperations.createWriter(
					getTablename(),
					true);
			synchronized (this) {
				if (!iteratorsAttached) {
					iteratorsAttached = true;
					final IteratorConfig[] configs = getIteratorConfig();
					if ((configs != null) && (configs.length > 0)) {
						accumuloOperations.attachIterators(
								getTablename(),
								true,
								true,
								true,
								null,
								configs);
					}
				}
			}

			final Mutation mutation = new Mutation(
					new Text(
							id.getBytes()));
			final Text cf = getSafeText(getColumnFamily());
			final Text cq = getSafeText(getColumnQualifier(object));
			final byte[] visibility = getVisibility(object);
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
			try {
				writer.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close metadata writer",
						e);
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable add object",
					e);
		}
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
			LOGGER.info(
					"Unable to find objects, table '" + getTablename() + "' does not exist",
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
			LOGGER.info(
					"Unable to find objects, table '" + getTablename() + "' does not exist",
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
			addObjectToCache(
					getPrimaryId(result),
					getSecondaryId(result),
					result);
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
				getTablename(),
				authorizations);
		final IteratorSetting[] settings = getScanSettings();
		if ((settings != null) && (settings.length > 0)) {
			for (final IteratorSetting setting : settings) {
				scanner.addScanIterator(setting);
			}
		}
		final String columnFamily = getColumnFamily();
		final String columnQualifier = getColumnQualifier(secondaryId);
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

	public boolean deleteObjects(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjects(
				null,
				secondaryId,
				authorizations);
	}

	@Override
	public boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {

		if (primaryId != null) {
			return accumuloOperations.delete(
					getTablename(),
					primaryId,
					getColumnFamily(),
					getColumnQualifier(secondaryId),
					authorizations);
		}

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
			catch (final IOException e) {
				LOGGER.error(
						"Unable to delete objects",
						e);
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to find objects, table '" + getTablename() + "' does not exist",
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
				final String[] authorizations ) {
			this.it = it;
			this.authorizations = authorizations;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			final Map.Entry<Key, Value> entry = it.next();
			accumuloOperations.delete(
					getTablename(),
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
