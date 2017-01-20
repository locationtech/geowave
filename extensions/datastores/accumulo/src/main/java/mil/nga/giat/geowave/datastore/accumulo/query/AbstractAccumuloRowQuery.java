package mil.nga.giat.geowave.datastore.accumulo.query;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;

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
			final ScanCallback<T> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				index,
				visibilityCounts,
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
				new AccumuloEntryIteratorWrapper(
						useWholeRowIterator(),
						adapterStore,
						index,
						scanner.iterator(),
						null,
						this.scanCallback));
	}

	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		addFieldSubsettingToIterator(scanner);
		if (useWholeRowIterator()) {
			// we have to at least use a whole row iterator
			final IteratorSetting iteratorSettings = new IteratorSetting(
					QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
					QueryFilterIterator.QUERY_ITERATOR_NAME,
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);
		}
	}

	abstract protected Integer getScannerLimit();
}
