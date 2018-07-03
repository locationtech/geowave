package mil.nga.giat.geowave.core.store.base;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderClosableWrapper;
import mil.nga.giat.geowave.core.store.util.NativeEntryTransformer;

/**
 * Represents a query operation by an Accumulo row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract class AbstractBaseRowQuery<T> extends
		BaseQuery
{
	private static final Logger LOGGER = Logger.getLogger(AbstractBaseRowQuery.class);
	protected final ScanCallback<T, ?> scanCallback;

	public AbstractBaseRowQuery(
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				index,
				visibilityCounts,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final double[] maxResolutionSubsamplingPerDimension,
			final PersistentAdapterStore adapterStore,
			final Integer limit ) {
		Reader<T> reader = getReader(
				operations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit,
				new NativeEntryTransformer<T>(
						adapterStore,
						index,
						getClientFilter(options),
						scanCallback,
						getFieldBitmask(),
						maxResolutionSubsamplingPerDimension,
						!isCommonIndexAggregation()));
		return new CloseableIteratorWrapper<T>(
				new ReaderClosableWrapper(
						reader),
				reader);
	}
}
