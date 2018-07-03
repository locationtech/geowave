package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderClosableWrapper;
import mil.nga.giat.geowave.core.store.util.MergingEntryIterator;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;

abstract class BaseFilteredIndexQuery extends
		BaseQuery
{
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(BaseFilteredIndexQuery.class);
	protected final ScanCallback<?, ?> scanCallback;

	public BaseFilteredIndexQuery(
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final ScanCallback<?, ?> scanCallback,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);
		this.scanCallback = scanCallback;
	}

	protected List<QueryFilter> getClientFilters() {
		return clientFilters;
	}

	@SuppressWarnings("rawtypes")
	public CloseableIterator<Object> query(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final Reader reader = getReader(
				datastoreOperations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit);
		if (reader == null) {
			return new CloseableIterator.Empty<Object>();
		}
		Iterator it = initIterator(
				options,
				adapterStore,
				reader,
				maxResolutionSubsamplingPerDimension,
				!isCommonIndexAggregation());
		if ((limit != null) && (limit > 0)) {
			it = Iterators.limit(
					it,
					limit);
		}
		return new CloseableIteratorWrapper(
				new ReaderClosableWrapper(
						reader),
				it);
	}

	@Override
	protected Reader getReader(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		boolean exists = false;
		try {
			exists = datastoreOperations.indexExists(index.getId());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Table does not exist",
					e);
		}
		if (!exists) {
			LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
			return null;
		}

		return super.getReader(
				datastoreOperations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit);
	}

	protected Map<Short, RowMergingDataAdapter> getMergingAdapters(
			final PersistentAdapterStore adapterStore ) {
		final Map<Short, RowMergingDataAdapter> mergingAdapters = new HashMap<Short, RowMergingDataAdapter>();
		for (final Short adapterId : adapterIds) {
			final DataAdapter<?> adapter = adapterStore.getAdapter(
					adapterId).getAdapter();
			if ((adapter instanceof RowMergingDataAdapter)
					&& (((RowMergingDataAdapter) adapter).getTransform() != null)) {
				mergingAdapters.put(
						adapterId,
						(RowMergingDataAdapter) adapter);
			}
		}

		return mergingAdapters;
	}

	protected Iterator initIterator(
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final Reader reader,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		// TODO GEOWAVE-1018: this will be a logical place to subsample (and
		// field subset?) if it is not already happening on the server

		// Determine client-side row merging
		if (!options.isServerSideLibraryEnabled()) {
			final Map<Short, RowMergingDataAdapter> mergingAdapters = getMergingAdapters(adapterStore);

			if (!mergingAdapters.isEmpty()) {
				return new MergingEntryIterator(
						adapterStore,
						index,
						reader,
						getClientFilter(options),
						scanCallback,
						mergingAdapters,
						maxResolutionSubsamplingPerDimension);
			}
		}

		return new NativeEntryIteratorWrapper(
				adapterStore,
				index,
				reader,
				getClientFilter(options),
				scanCallback,
				getFieldBitmask(),
				maxResolutionSubsamplingPerDimension,
				decodePersistenceEncoding);
	}

	@Override
	protected QueryFilter getClientFilter(
			final DataStoreOptions options ) {
		final List<QueryFilter> internalClientFilters = getClientFiltersList(options);
		return internalClientFilters.isEmpty() ? null : internalClientFilters.size() == 1 ? internalClientFilters
				.get(0) : new FilterList<QueryFilter>(
				internalClientFilters);
	}

	protected List<QueryFilter> getClientFiltersList(
			final DataStoreOptions options ) {
		return clientFilters;
	}
}
