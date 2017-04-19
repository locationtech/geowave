package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.ScannerClosableWrapper;

abstract public class AbstractHBaseRowQuery<T> extends
		HBaseQuery
{

	private static final Logger LOGGER = Logger.getLogger(AbstractHBaseRowQuery.class);
	protected final ScanCallback<T, ?> scanCallback;

	public AbstractHBaseRowQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				dataStore,
				index,
				visibilityCounts,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final BasicHBaseOperations operations,
			final double[] maxResolutionSubsamplingPerDimension,
			final AdapterStore adapterStore ) {
		final Scan scanner = new Scan();
		scanner.setMaxResultSize(getScannerLimit());
		ResultScanner results = null;
		try {
			results = operations.getScannedResults(
					scanner,
					StringUtils.stringFromBinary(index.getId().getBytes()),
					authorizations);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to get the scanned results.",
					e);
		}

		if (results != null) {
			return new CloseableIteratorWrapper<T>(
					new ScannerClosableWrapper(
							results),
					new HBaseEntryIteratorWrapper(
							dataStore,
							adapterStore,
							index,
							results.iterator(),
							null, // no client filter
							null, // no scan callback
							fieldIdsAdapterPair,
							maxResolutionSubsamplingPerDimension,
							true,
							false));
		}
		else {
			return new CloseableIterator.Empty<T>();
		}
	}

	abstract protected Integer getScannerLimit();
}
