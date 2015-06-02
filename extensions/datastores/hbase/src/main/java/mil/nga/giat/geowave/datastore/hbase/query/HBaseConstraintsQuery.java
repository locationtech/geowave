/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloConstraintsQuery </code>
 */
public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(HBaseConstraintsQuery.class);
	private static final int MAX_RANGE_DECOMPOSITION = 5000;
	private MultiDimensionalNumericData constraints;
	protected final List<DistributableQueryFilter> distributableFilters;
	protected boolean queryFiltersEnabled;

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				constraints,
				queryFilters,
				null,
				null,
				authorizations);

	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		this(
				adapterIds,
				index,
				null,
				null,
				clientDedupeFilter,
				scanCallback,
				authorizations);
	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				scanCallback,
				authorizations);
		this.constraints = constraints;
		final SplitFilterLists lists = splitList(queryFilters);
		final List<QueryFilter> clientFilters = lists.clientFilters;
		// add dedupe filters to the front of both lists so that the
		// de-duplication is performed before any more complex filtering
		// operations, use the supplied client dedupe filter if possible
		clientFilters.add(
				0,
				clientDedupeFilter != null ? clientDedupeFilter : new DedupeFilter());
		super.setClientFilters(clientFilters);
		distributableFilters = lists.distributableFilters;
		// we are assuming we always have to ensure no duplicates
		// and that the deduplication is the least expensive filter so we add it
		// first
		distributableFilters.add(
				0,
				new DedupeFilter());
		queryFiltersEnabled = true;
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
		return HBaseUtils.constraintsToByteArrayRanges(
				constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION);
	}

	@Override
	protected List<QueryFilter> getAllFiltersList() {
		List<QueryFilter> filters = super.getAllFiltersList();
		filters.addAll(distributableFilters);
		return filters;
	}

	@Override
	protected List<Filter> getDistributableFilter() {
		return new ArrayList<Filter>();
	}
}
