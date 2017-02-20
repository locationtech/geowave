package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeQueryFilter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

/**
 * This class represents basic numeric contraints applied to a datastore query
 *
 */
class BaseConstraintsQuery extends
		BaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(BaseConstraintsQuery.class);
	private boolean queryFiltersEnabled;

	public final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	public final List<MultiDimensionalNumericData> constraints;
	public final List<DistributableQueryFilter> distributableFilters;

	public final IndexMetaData[] indexMetaData;
	private final PrimaryIndex index;

	public BaseConstraintsQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		this(
				dataStore,
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index.getIndexStrategy()) : null,
				query != null ? query.createFilters(index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				fieldIdsAdapterPair,
				indexMetaData,
				duplicateCounts,
				visibilityCounts,
				authorizations);
	}

	public BaseConstraintsQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {

		super(
				dataStore,
				adapterIds,
				index,
				scanCallback,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);
		this.constraints = constraints;
		this.aggregation = aggregation;
		this.indexMetaData = indexMetaData != null ? indexMetaData : new IndexMetaData[] {};
		this.index = index;
		final SplitFilterLists lists = splitList(queryFilters);
		final List<QueryFilter> clientFilters = lists.clientFilters;
		if ((duplicateCounts != null) && !duplicateCounts.isAnyEntryHaveDuplicates()) {
			clientDedupeFilter = null;
		}
		// add dedupe filters to the front of both lists so that the
		// de-duplication is performed before any more complex filtering
		// operations, use the supplied client dedupe filter if possible
		if (clientDedupeFilter != null) {
			clientFilters.add(
					0,
					clientDedupeFilter);
		}
		this.clientFilters = clientFilters;
		distributableFilters = lists.distributableFilters;
		if (clientDedupeFilter != null) {
			distributableFilters.add(
					0,
					clientDedupeFilter);
		}
		queryFiltersEnabled = true;
	}

	@Override
	public DistributableQueryFilter getServerFilter(
			final DataStoreOptions options ) {
		// TODO GEOWAVE-1018 is options necessary? is this correct?
		if ((distributableFilters == null) || distributableFilters.isEmpty()) {
			return null;
		}
		else if (distributableFilters.size() > 1) {
			return new DistributableFilterList(
					distributableFilters);
		}
		else {
			return distributableFilters.get(0);
		}
	}

	public boolean isQueryFiltersEnabled() {
		return queryFiltersEnabled;
	}

	public void setQueryFiltersEnabled(
			final boolean queryFiltersEnabled ) {
		this.queryFiltersEnabled = queryFiltersEnabled;
	}

	@Override
	public CloseableIterator<Object> query(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		if (isAggregation()) {
			if ((options == null) || !options.isServerSideLibraryEnabled()) {
				// Aggregate client-side
				final CloseableIterator<Object> it = super.query(
						datastoreOperations,
						options,
						adapterStore,
						maxResolutionSubsamplingPerDimension,
						limit);

				if ((it != null) && it.hasNext()) {
					final Aggregation aggregationFunction = aggregation.getRight();
					synchronized (aggregationFunction) {
						aggregationFunction.clearResult();
						while (it.hasNext()) {
							final Object input = it.next();
							if (input != null) {
								aggregationFunction.aggregate(input);
							}
						}
						try {
							it.close();
						}
						catch (final IOException e) {
							LOGGER.warn(
									"Unable to close datastore reader",
									e);
						}

						return new Wrapper(
								Iterators.singletonIterator(aggregationFunction.getResult()));
					}
				}

				return new CloseableIterator.Empty();
			}
			else {
				// the aggregation is run server-side use the reader to
				// aggregate to a single value here
				try (final Reader reader = getReader(
						datastoreOperations,
						options,
						maxResolutionSubsamplingPerDimension,
						limit)) {
					Mergeable mergedAggregationResult = null;
					if (reader == null || !reader.hasNext()) {
						return new CloseableIterator.Empty();
					}
					else {
						while (reader.hasNext()) {
							final GeoWaveRow row = reader.next();
							for (final GeoWaveValue value : row.getFieldValues()) {
								if ((value.getValue() != null) && (value.getValue().length > 0)) {
									if (mergedAggregationResult == null) {
										mergedAggregationResult = PersistenceUtils.fromBinary(
												value.getValue(),
												Mergeable.class);
									}
									else {
										mergedAggregationResult.merge(PersistenceUtils.fromBinary(
												value.getValue(),
												Mergeable.class));
									}
								}
							}
						}
						return new CloseableIterator.Wrapper<>(
								Iterators.singletonIterator(mergedAggregationResult));
					}
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to close reader for aggregation",
							e);
				}
			}
		}
		return super.query(
				datastoreOperations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit);
	}

	@Override
	protected List<QueryFilter> getClientFiltersList(
			final DataStoreOptions options ) {

		// Since we have custom filters enabled, this list should only return
		// the client filters
		if ((options != null) && options.isServerSideLibraryEnabled()) {
			return clientFilters;
		}
		// add a index filter to the front of the list if there isn't already a
		// filter
		if (distributableFilters.isEmpty()
				|| ((distributableFilters.size() == 1) && (distributableFilters.get(0) instanceof DedupeFilter))) {
			final List<MultiDimensionalCoordinateRangesArray> coords = getCoordinateRanges();
			if (!coords.isEmpty()) {
				clientFilters.add(
						0,
						new CoordinateRangeQueryFilter(
								index.getIndexStrategy(),
								coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})));
			}
		}
		else {
			// Without custom filters, we need all the filters on the client
			// side
			for (final QueryFilter distributable : distributableFilters) {
				if (!clientFilters.contains(distributable)) {
					clientFilters.add(distributable);
				}
			}
		}
		return clientFilters;
	}

	@Override
	protected boolean isCommonIndexAggregation() {
		return isAggregation() && (aggregation.getRight() instanceof CommonIndexAggregation);
	}

	@Override
	protected Pair<DataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	@Override
	public List<MultiDimensionalNumericData> getConstraints() {
		return constraints;
	}

	@Override
	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<MultiDimensionalCoordinateRangesArray>();
		}
		else {
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final List<MultiDimensionalCoordinateRangesArray> ranges = new ArrayList<MultiDimensionalCoordinateRangesArray>();
			for (final MultiDimensionalNumericData nd : constraints) {
				ranges.add(new MultiDimensionalCoordinateRangesArray(
						indexStrategy.getCoordinateRangesPerDimension(
								nd,
								indexMetaData)));
			}
			return ranges;
		}
	}

	@Override
	protected QueryRanges getRanges() {
		if (isAggregation()) {
			final QueryRanges ranges = DataStoreUtils.constraintsToQueryRanges(
					constraints,
					index.getIndexStrategy(),
					BaseDataStoreUtils.AGGREGATION_RANGE_DECOMPOSITION,
					indexMetaData);

			return ranges;
		}
		else {
			return getAllRanges();
		}
	}

	public QueryRanges getAllRanges() {
		return DataStoreUtils.constraintsToQueryRanges(
				constraints,
				index.getIndexStrategy(),
				BaseDataStoreUtils.MAX_RANGE_DECOMPOSITION,
				indexMetaData);
	}

	private SplitFilterLists splitList(
			final List<QueryFilter> allFilters ) {
		final List<DistributableQueryFilter> distributableFilters = new ArrayList<DistributableQueryFilter>();
		final List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
		if ((allFilters == null) || allFilters.isEmpty()) {
			return new SplitFilterLists(
					distributableFilters,
					clientFilters);
		}
		for (final QueryFilter filter : allFilters) {
			if (filter instanceof DistributableQueryFilter) {
				distributableFilters.add((DistributableQueryFilter) filter);
			}
			else {
				clientFilters.add(filter);
			}
		}
		return new SplitFilterLists(
				distributableFilters,
				clientFilters);
	}

	private static class SplitFilterLists
	{
		private final List<DistributableQueryFilter> distributableFilters;
		private final List<QueryFilter> clientFilters;

		public SplitFilterLists(
				final List<DistributableQueryFilter> distributableFilters,
				final List<QueryFilter> clientFilters ) {
			this.distributableFilters = distributableFilters;
			this.clientFilters = clientFilters;
		}
	}
}
