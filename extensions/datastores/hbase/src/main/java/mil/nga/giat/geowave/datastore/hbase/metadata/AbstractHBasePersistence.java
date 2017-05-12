package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.ScannerClosableWrapper;

public abstract class AbstractHBasePersistence<T extends Persistable> extends
		AbstractGeowavePersistence<T>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractHBasePersistence.class);
	protected static final String[] METADATA_CFS = new String[] {
		HBaseAdapterIndexMappingStore.ADAPTER_INDEX_CF,
		HBaseAdapterStore.ADAPTER_CF,
		HBaseDataStatisticsStore.STATISTICS_CF,
		HBaseIndexStore.INDEX_CF
	};
	protected final BasicHBaseOperations operations;

	public AbstractHBasePersistence(
			final BasicHBaseOperations operations ) {
		super(
				operations);
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
					getTablename(),
					authorizations).iterator();

			final Iterator<T> iter = getNativeIteratorWrapper(it);

			if (!iter.hasNext()) {
				LOGGER.warn("Object '" + getCombinedId(
						primaryId,
						secondaryId).getString() + "' not found");
				return null;
			}

			return iter.next();
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
			addObjectToCache(
					getPrimaryId(result),
					getSecondaryId(result),
					result);
		}
		return result;
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

	protected Iterator<T> getNativeIteratorWrapper(
			final Iterator<Result> resultIterator ) {
		return new NativeIteratorWrapper(
				resultIterator);
	}

	protected Scan applyScannerSettings(
			final Scan scanner,
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {

		final String columnFamily = getColumnFamily();
		final String columnQualifier = getColumnQualifier(secondaryId);
		if (columnFamily != null) {
			if (columnQualifier != null) {
				scanner.addColumn(
						toBytes(columnFamily),
						toBytes(columnQualifier));
			}
			else {
				scanner.addFamily(toBytes(columnFamily));
			}
		}
		if (primaryId != null) {
			scanner.setStartRow(primaryId.getBytes());
			scanner.setStopRow(primaryId.getBytes());
		}
		return scanner;
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		try {
			final Scan scanner = getFullScanner(authorizations);
			final ResultScanner rS = operations.getScannedResults(
					scanner,
					getTablename(),
					authorizations);
			final Iterator<Result> it = rS.iterator();
			return new CloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							rS),
					getNativeIteratorWrapper(it));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to find objects in HBase table.",
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

	protected ByteArrayId getRowId(
			final T object ) {
		return getPrimaryId(object);
	}

	@Override
	protected void addObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final T object ) {
		final ByteArrayId id = getRowId(object);
		addObjectToCache(
				primaryId,
				secondaryId,
				object);
		try {

			final HBaseWriter writer = operations.createWriter(
					getTablename(),
					// create table with all possible column families initially
					// because it is known
					METADATA_CFS,
					true);
			final RowMutations mutation = new RowMutations(
					id.getBytes());
			final Put row = new Put(
					id.getBytes());
			row.addColumn(
					toBytes(getColumnFamily()),
					toBytes(getColumnQualifier(object)),
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

	public boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		try {
			// Only way to do this is with a Scanner.
			final Scan scanner = getScanner(
					primaryId,
					secondaryId,
					authorizations);
			final ResultScanner rS = operations.getScannedResults(
					scanner,
					getTablename(),
					authorizations);

			final byte[] columnFamily = toBytes(getColumnFamily());
			final byte[] columnQualifier = toBytes(getColumnQualifier(secondaryId));

			final List<RowMutations> l = new ArrayList<RowMutations>();
			for (final Result rr : rS) {

				final RowMutations deleteMutations = HBaseUtils.getDeleteMutations(
						rr.getRow(),
						columnFamily,
						columnQualifier,
						authorizations);

				l.add(deleteMutations);

			}
			try (final HBaseWriter deleter = operations.createWriter(
					getTablename(),
					METADATA_CFS,
					false)) {

				deleter.delete(l);
			}
			return true;
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to delete row from " + getTablename(),
					e);
			return false;
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
			final Cell cell = it.next().listCells().get(
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
			final ResultScanner rS = operations.getScannedResults(
					scanner,
					getTablename(),
					authorizations);
			final Iterator<Result> it = rS.iterator();
			return new CloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							rS),
					getNativeIteratorWrapper(it));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to find objects, table '" + getTablename() + "' does not exist",
					e);
		}
		return new CloseableIterator.Empty<T>();
	}
}