package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(HBaseConstraintsQuery.class);
	private static final int MAX_RANGE_DECOMPOSITION = 5000;
	private final List<MultiDimensionalNumericData> constraints;
	protected final List<DistributableQueryFilter> distributableFilters;
	protected boolean queryFiltersEnabled;

	// TODO How to use?
	protected final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index.getIndexStrategy()) : null,
				query != null ? query.createFilters(index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				authorizations);
	}

	public HBaseConstraintsQuery(
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters ) {
		this(
				null,
				index,
				constraints,
				queryFilters,
				new String[0]);
	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters ) {
		this(
				adapterIds,
				index,
				constraints,
				queryFilters,
				(DedupeFilter) null,
				(ScanCallback<?>) null,
				null,
				new String[0]);

	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				constraints,
				queryFilters,
				(DedupeFilter) null,
				(ScanCallback<?>) null,
				null,
				authorizations);

	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				scanCallback,
				authorizations);
		this.constraints = constraints;
		this.aggregation = aggregation;
		final SplitFilterLists lists = splitList(queryFilters);
		final List<QueryFilter> clientFilters = lists.clientFilters;
		// add dedupe filters to the front of both lists so that the
		// de-duplication is performed before any more complex filtering
		// operations, use the supplied client dedupe filter if possible
		if (clientDedupeFilter != null) {
			clientFilters.add(
					0,
					clientDedupeFilter);
		}
		super.setClientFilters(clientFilters);
		distributableFilters = lists.distributableFilters;
		if (!distributableFilters.isEmpty() && (clientDedupeFilter != null)) {
			distributableFilters.add(
					0,
					clientDedupeFilter);
		}
		queryFiltersEnabled = true;
		if (isAggregation()) {
			// Because aggregations are done client-side make sure to set
			// the adapter ID here
			this.adapterIds = Collections.singletonList(aggregation.getLeft().getAdapterId());
		}
	}

	protected boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getLeft() != null) && (aggregation.getRight() != null));

	}

	private static SplitFilterLists splitList(
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

	@Override
	protected List<ByteArrayRange> getRanges() {
		return DataStoreUtils.constraintsToByteArrayRanges(
				constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION);
	}

	@Override
	protected List<QueryFilter> getAllFiltersList() {
		final List<QueryFilter> filters = super.getAllFiltersList();
		for (final QueryFilter distributable : distributableFilters) {
			if (!filters.contains(distributable)) {
				filters.add(distributable);
			}
		}
		return filters;
	}

	@Override
	protected List<Filter> getDistributableFilter() {
		return new ArrayList<Filter>();
	}

	@Override
	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final CloseableIterator<Object> it = super.query(
				operations,
				adapterStore,
				limit);
		if (isAggregation() && (it != null) && it.hasNext()) {
			// TODO implement aggregation as a co-processor on the server side,
			// but for now simply aggregate client-side here

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
							"Unable to close hbase scanner",
							e);
				}
				return new Wrapper(
						Iterators.singletonIterator(aggregationFunction.getResult()));
			}
		}
		else {
			return super.query(
					operations,
					adapterStore,
					limit);
		}
	}
}
