package mil.nga.giat.geowave.core.store.base;

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
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
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
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
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
public class BaseConstraintsQuery extends
		BaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(BaseConstraintsQuery.class);
	private boolean queryFiltersEnabled;

	public final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	public final List<MultiDimensionalNumericData> constraints;
	public final List<DistributableQueryFilter> distributableFilters;

	public final IndexMetaData[] indexMetaData;
	private final PrimaryIndex index;

	public BaseConstraintsQuery(
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index) : null,
				query != null ? query.createFilters(index) : null,
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
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
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
			clientFilters.add(clientDedupeFilter);
		}
		this.clientFilters = clientFilters;
		distributableFilters = lists.distributableFilters;

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
			final PersistentAdapterStore adapterStore,
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
				return BaseDataStoreUtils.aggregate(
						it,
						aggregation.getValue());
			}
			else {
				// the aggregation is run server-side use the reader to
				// aggregate to a single value here
				try (final Reader reader = getReader(
						datastoreOperations,
						options,
						adapterStore,
						maxResolutionSubsamplingPerDimension,
						limit)) {
					Mergeable mergedAggregationResult = null;
					if ((reader == null) || !reader.hasNext()) {
						return new CloseableIterator.Empty();
					}
					else {
						while (reader.hasNext()) {
							final GeoWaveRow row = reader.next();
							for (final GeoWaveValue value : row.getFieldValues()) {
								if ((value.getValue() != null) && (value.getValue().length > 0)) {
									if (mergedAggregationResult == null) {
										mergedAggregationResult = (Mergeable) PersistenceUtils.fromBinary(value
												.getValue());
									}
									else {
										mergedAggregationResult.merge((Mergeable) PersistenceUtils.fromBinary(value
												.getValue()));
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
	protected Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
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
