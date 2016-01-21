package mil.nga.giat.geowave.datastore.accumulo.query;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.EntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.log4j.Logger;

/**
 * Represents a query operation by an Accumulo row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract public class AbstractAccumuloRowQuery<T> extends
		AccumuloQuery
{
	private static final Logger LOGGER = Logger.getLogger(AbstractAccumuloRowQuery.class);
	protected final ScanCallback<T> scanCallback;

	public AbstractAccumuloRowQuery(
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T> scanCallback ) {
		super(
				index,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final AccumuloOperations accumuloOperations,
			final double[] maxResolutionSubsamplingPerDimension,
			final AdapterStore adapterStore ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				maxResolutionSubsamplingPerDimension,
				getScannerLimit());
		if (scanner == null) {
			LOGGER.error("Unable to get a new scanner instance, getScanner returned null");
			return null;
		}
		addScanIteratorSettings(scanner);
		return new CloseableIteratorWrapper<T>(
				new ScannerClosableWrapper(
						scanner),
				new EntryIteratorWrapper(
						adapterStore,
						index,
						scanner.iterator(),
						null,
						this.scanCallback));
	}

	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		// we have to at least use a whole row iterator
		final IteratorSetting iteratorSettings = new IteratorSetting(
				QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
				QueryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
				WholeRowIterator.class);
		scanner.addScanIterator(iteratorSettings);
	}

	abstract protected Integer getScannerLimit();
}
