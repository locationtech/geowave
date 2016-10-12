package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class ConstraintsQuery
{
	public static final int MAX_RANGE_DECOMPOSITION = 2000;

	public final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	public final List<MultiDimensionalNumericData> constraints;
	public final List<DistributableQueryFilter> distributableFilters;

	private final IndexMetaData[] indexMetaData;
	private final PrimaryIndex index;

	public ConstraintsQuery(
			final List<MultiDimensionalNumericData> constraints,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final PrimaryIndex index,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final DuplicateEntryCount duplicateCounts,
			final FilteredIndexQuery parentQuery ) {
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
		parentQuery.setClientFilters(clientFilters);
		distributableFilters = lists.distributableFilters;
		if (!distributableFilters.isEmpty() && (clientDedupeFilter != null)) {
			distributableFilters.add(
					0,
					clientDedupeFilter);
		}
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getLeft() != null) && (aggregation.getRight() != null));
	}

	public List<ByteArrayRange> getRanges() {
		if (isAggregation()) {
			final List<ByteArrayRange> ranges = DataStoreUtils.constraintsToByteArrayRanges(
					constraints,
					index.getIndexStrategy(),
					MAX_RANGE_DECOMPOSITION,
					indexMetaData);
			if ((ranges == null) || (ranges.size() < 2)) {
				return ranges;
			}
			ByteArrayId start = null;
			ByteArrayId end = null;

			for (final ByteArrayRange range : ranges) {
				if ((start == null) || (range.getStart().compareTo(
						start) < 0)) {
					start = range.getStart();
				}
				if ((end == null) || (range.getEnd().compareTo(
						end) > 0)) {
					end = range.getEnd();
				}
			}
			final List<ByteArrayRange> retVal = new ArrayList<ByteArrayRange>();
			retVal.add(new ByteArrayRange(
					start,
					end));
			return retVal;
		}
		else {
			return getAllRanges();
		}
	}

	public List<ByteArrayRange> getAllRanges() {
			return DataStoreUtils.constraintsToByteArrayRanges(
					constraints,
					index.getIndexStrategy(),
					MAX_RANGE_DECOMPOSITION,
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
