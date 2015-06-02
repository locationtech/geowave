/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to
 *         <code> AbstractAccumuloPersistence </code>
 */
public abstract class AbstractHBasePersistence<T extends Persistable>
{

	public final static String METADATA_TABLE = "GEOWAVE_METADATA";
	private final static Logger LOGGER = Logger.getLogger(AbstractHBasePersistence.class);
	private final BasicHBaseOperations operations;

	private static final int MAX_ENTRIES = 100;
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

	public AbstractHBasePersistence(
			final BasicHBaseOperations operations ) {
		this.operations = operations;
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
		final Scan scanner = getScanner(
				primaryId,
				secondaryId,
				authorizations);
		try {
			final Iterator<Result> it = operations.getScannedResults(
					scanner,
					getTablename()).iterator();
			if (!it.hasNext()) {
				LOGGER.warn("Object '" + getCombinedId(
						primaryId,
						secondaryId).getString() + "' not found");
				return null;
			}
			final Result entry = it.next();
			return entryToValue(entry.listCells().get(
					0));
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to find object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "'",
					e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	protected T entryToValue(
			final Cell entry ) {
		final T result = (T) PersistenceUtils.fromBinary(
				CellUtil.cloneValue(entry),
				Persistable.class);
		if (result != null) {
			addObjectToCache(result);
		}
		return result;
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

	abstract protected ByteArrayId getPrimaryId(
			T persistedObject );

	protected ByteArrayId getSecondaryId(
			final T persistedObject ) {
		return null;
	}

	protected ByteArrayId getSecondaryId(
			final byte[] key ) {
		return new ByteArrayId(
				key);
	}

	protected ByteArrayId getPrimaryId(
			final byte[] row ) {
		return new ByteArrayId(
				row);
	}

	protected Scan getScanner(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final Scan scanner = new Scan();
		applyScannerSettings(
				scanner,
				primaryId,
				secondaryId);
		return scanner;
	}

	protected String getColumnFamily() {
		return getPersistenceTypeName();
	}

	private byte[] toBytes(
			String s ) {
		return s.getBytes(Charset.forName("UTF-8"));
	}

	abstract protected String getPersistenceTypeName();

	protected byte[] getColumnQualifier(
			final T persistedObject ) {
		return getColumnQualifier(getSecondaryId(persistedObject));
	}

	protected byte[] getColumnQualifier(
			final ByteArrayId secondaryId ) {
		if (secondaryId != null) {
			return secondaryId.getBytes();
		}
		return null;
	}

	protected Scan applyScannerSettings(
			Scan scanner,
			ByteArrayId primaryId,
			ByteArrayId secondaryId ) {

		final byte[] columnFamily = toBytes(getColumnFamily());
		final byte[] columnQualifier = getColumnQualifier(secondaryId);
		if (columnFamily != null) {
			if (columnQualifier != null) {
				scanner.addColumn(
						columnFamily,
						columnQualifier);
			}
			else {
				scanner.addFamily(columnFamily);
			}
		}
		if (primaryId != null) {
			scanner.setStartRow(primaryId.getBytes());
		}
		return scanner;
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

	protected String getTablename() {
		return METADATA_TABLE;
	}

	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);
		return cache.get(combinedId);
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		try {
			final Scan scanner = getFullScanner(authorizations);
			ResultScanner rS = operations.getScannedResults(
					scanner,
					getTablename());
			final Iterator<Result> it = rS.iterator();
			return new HBaseCloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							rS),
					new NativeIteratorWrapper(
							it));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to find objects, table '" + getTablename() + "' does not exist",
					e);
		}
		return new CloseableIterator.Empty<T>();
	}

	private Scan getFullScanner(
			final String... authorizations )
			throws TableNotFoundException {
		return getScanner(
				null,
				null,
				authorizations);
	}

	protected void addObject(
			final T object ) {
		final ByteArrayId id = getPrimaryId(object);
		addObjectToCache(object);
		try {

			final HBaseWriter writer = operations.createWriter(
					getTablename(),
					getColumnFamily().toString(),
					true);
			final RowMutations mutation = new RowMutations(
					id.getBytes());
			Put row = new Put(
					id.getBytes());
			row.addColumn(
					toBytes(getColumnFamily()),
					getColumnQualifier(object),
					PersistenceUtils.toBinary(object));
			mutation.add(row);

			writer.write(
					mutation,
					getColumnFamily().toString());
			writer.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable add object",
					e);
		}
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
			final Scan scanner = getScanner(
					primaryId,
					secondaryId);
			ResultScanner rS = operations.getScannedResults(
					scanner,
					getTablename());
			final Iterator<Result> it = rS.iterator();
			if (it.hasNext()) {
				// may as well cache the result
				Cell c = it.next().listCells().get(
						0);
				return (entryToValue(c) != null);
			}
			else {
				return false;
			}
		}
		catch (final IOException e) {
			LOGGER.debug(
					"Unable to check existence of object '" + getCombinedId(
							primaryId,
							secondaryId) + "'",
					e);
		}
		return false;
	}

	private class NativeIteratorWrapper implements
			Iterator<T>
	{
		final private Iterator<Result> it;

		private NativeIteratorWrapper(
				final Iterator<Result> it ) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public T next() {
			Cell cell = it.next().listCells().get(
					0);
			return entryToValue(cell);
		}

		@Override
		public void remove() {
			it.remove();
		}

	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		try {
			final Scan scanner = getScanner(
					null,
					secondaryId,
					authorizations);
			ResultScanner rS = operations.getScannedResults(
					scanner,
					getTablename());
			final Iterator<Result> it = rS.iterator();
			return new HBaseCloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							rS),
					new NativeIteratorWrapper(
							it));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to find objects, table '" + getTablename() + "' does not exist",
					e);
		}
		return new CloseableIterator.Empty<T>();
	}

	protected boolean deleteObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjectFromCache(
				primaryId,
				secondaryId) && deleteRows(
				primaryId,
				secondaryId,
				authorizations);
	}

	private boolean deleteRows(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		try {
			RowMutations deleteMutations = HBaseUtils.getDeleteMutations(
					primaryId.getBytes(),
					getColumnFamily().getBytes(
							Charset.forName("UTF-8")),
					getColumnQualifier(secondaryId),
					authorizations);
			HBaseWriter deleter = operations.createWriter(
					getTablename(),
					getColumnFamily(),
					false);
			List<RowMutations> l = new ArrayList<RowMutations>();
			l.add(deleteMutations);
			deleter.delete(l);
			return true;
		}
		catch (IOException e) {
			LOGGER.warn("Unable to delete row from " + getTablename() + " " + e);
			return false;
		}
	}

	protected boolean deleteObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		final ByteArrayId combinedId = getCombinedId(
				primaryId,
				secondaryId);
		return (cache.remove(combinedId) != null);
	}
}