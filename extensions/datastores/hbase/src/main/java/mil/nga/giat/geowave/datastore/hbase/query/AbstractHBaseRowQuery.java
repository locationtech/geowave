/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;

/**
 * @author viggy Functionality similar to
 *         <code> AbstractAccumuloRowQuery </code>
 */
abstract public class AbstractHBaseRowQuery<T> extends
		HBaseQuery
{

	private static final Logger LOGGER = Logger.getLogger(
			AbstractHBaseRowQuery.class);
	protected final ScanCallback<T> scanCallback;

	public AbstractHBaseRowQuery(
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T> scanCallback ) {
		super(
				index);
		this.scanCallback = scanCallback;
	}

	public T query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore ) {
		final Scan scanner = new Scan();
		scanner.setMaxResultSize(
				getScannerLimit());
		ResultScanner results = null;
		try {
			results = operations.getScannedResults(
					scanner,
					StringUtils.stringFromBinary(
							index.getId().getBytes()));
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to get the scanned results " + e);
		}
		/*
		 * getScanner( accumuloOperations, getScannerLimit());
		 * addScanIteratorSettings(scanner);
		 */
		final HBaseCloseableIteratorWrapper<Object> it = new HBaseCloseableIteratorWrapper<Object>(
				new ScannerClosableWrapper(
						results),
				new HBaseEntryIteratorWrapper(
						adapterStore,
						index,
						results.iterator(),
						null));
		return queryResultFromIterator(
				it);
	}

	abstract protected Integer getScannerLimit();

	abstract protected T queryResultFromIterator(
			final HBaseCloseableIteratorWrapper<?> it );
}
