/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloConstraintsQuery </code>
 */
public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(
			HBaseConstraintsQuery.class);
	private static final int MAX_RANGE_DECOMPOSITION = 5000;
	private final List<MultiDimensionalNumericData> constraints;
	protected final List<DistributableQueryFilter> distributableFilters;
	protected boolean queryFiltersEnabled;

	// TODO How to use?
	protected final Pair<DataAdapter<?>, Aggregation<?>> aggregation;
	
	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?>> aggregation,
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
		// TODO determine what to do with isSupported() - this at one
		// point acted as rudimentary query planning but it seems like a
		// responsibility of higher level query planning logic for example,
		// sometimes a spatial index is better at handling a spatial-temporal
		// query than a spatial-temporal index, if the constraints are much more
		// restrictive spatially

		// if ((query != null) && !query.isSupported(index)) {
		// throw new IllegalArgumentException(
		// "Index does not support the query");
		// }
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
			final Pair<DataAdapter<?>, Aggregation<?>> aggregation,
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
				distributableFilters.add(
						(DistributableQueryFilter) filter);
			}
			else {
				clientFilters.add(
						filter);
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
		for(QueryFilter distributable : distributableFilters){
			if(!filters.contains(distributable)){
				filters.add(distributable);
			}
		}
		return filters;
	}

	@Override
	protected List<Filter> getDistributableFilter() {
		return new ArrayList<Filter>();
	}
}
