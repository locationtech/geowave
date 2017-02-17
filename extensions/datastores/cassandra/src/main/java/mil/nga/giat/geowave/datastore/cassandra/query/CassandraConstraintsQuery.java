package mil.nga.giat.geowave.datastore.cassandra.query;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeQueryFilter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

/**
 * This class represents basic numeric contraints applied to an Cassandra Query
 *
 */
public class CassandraConstraintsQuery extends
		CassandraFilteredIndexQuery
{
	private static final Logger LOGGER = Logger.getLogger(CassandraConstraintsQuery.class);
	// TODO: determine good values for max range decomposition in cassandra
	private static final int MAX_RANGE_DECOMPOSITION = 10;
	protected final ConstraintsQuery base;
	private boolean queryFiltersEnabled;

	public CassandraConstraintsQuery(
			final BaseDataStore dataStore,
			final CassandraOperations operations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, CassandraRow> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		this(
				dataStore,
				operations,
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

	public CassandraConstraintsQuery(
			final BaseDataStore dataStore,
			final CassandraOperations operations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, CassandraRow> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				dataStore,
				operations,
				adapterIds,
				index,
				queryFilters,
				clientDedupeFilter,
				scanCallback,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);

		base = new ConstraintsQuery(
				constraints,
				aggregation,
				indexMetaData,
				index,
				queryFilters,
				clientDedupeFilter,
				duplicateCounts,
				this);

		queryFiltersEnabled = true;
	}

	@Override
	protected boolean isAggregation() {
		return base.isAggregation();
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		return DataStoreUtils.constraintsToByteArrayRanges(
				base.constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION,
				base.indexMetaData);
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
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final CloseableIterator<Object> results = super.query(
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit);
		if (isAggregation()) {
			// aggregate the stats to a single value here
			if (!results.hasNext()) {
				return new CloseableIterator.Wrapper<Object>(
						Iterators.emptyIterator());
			}
			else {
				final Aggregation aggregationFunction = base.aggregation.getRight();
				synchronized (aggregationFunction) {
					aggregationFunction.clearResult();
					while (results.hasNext()) {
						final Object input = results.next();
						if (input != null) {
							// TODO this is a hack for now
							if (aggregationFunction instanceof CommonIndexAggregation) {
								aggregationFunction.aggregate(null);
							}
							else {
								aggregationFunction.aggregate(input);
							}
						}
					}
					try {
						results.close();
					}
					catch (final IOException e) {
						LOGGER.warn(
								"Unable to close hbase scanner",
								e);
					}
					return new Wrapper(
							Iterators.singletonIterator(aggregationFunction.getResult()));
				}
			}
		}
		return results;
	}

	@Override
	protected List<QueryFilter> getAllFiltersList() {
		final List<QueryFilter> filters = super.getAllFiltersList();
		// add a index filter to the front of the list if there isn't already a
		// filter
		if (base.distributableFilters.isEmpty()) {
			final List<MultiDimensionalCoordinateRangesArray> coords = base.getCoordinateRanges();
			if (!coords.isEmpty()) {
				filters.add(
						0,
						new CoordinateRangeQueryFilter(
								index.getIndexStrategy(),
								coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})));
			}
		}
		else {
			// Without custom filters, we need all the filters on the client
			// side
			for (final QueryFilter distributable : base.distributableFilters) {
				if (!filters.contains(distributable)) {
					filters.add(distributable);
				}
			}
		}
		return filters;
	}
}
