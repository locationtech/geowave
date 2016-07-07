package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

/**
 * This class represents basic numeric contraints applied to an Accumulo Query
 *
 */
public class AccumuloConstraintsQuery extends
		AccumuloFilteredIndexQuery
{
	protected static final int MAX_RANGE_DECOMPOSITION = 5000;
	protected final List<MultiDimensionalNumericData> constraints;
	protected final List<DistributableQueryFilter> distributableFilters;
	protected boolean queryFiltersEnabled;
	protected final IndexMetaData[] indexMetaData;
	protected final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;

	public AccumuloConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		this(
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

	public AccumuloConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
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
		super.setClientFilters(clientFilters);
		distributableFilters = lists.distributableFilters;
		if (!distributableFilters.isEmpty() && (clientDedupeFilter != null)) {
			distributableFilters.add(
					0,
					clientDedupeFilter);
		}

		queryFiltersEnabled = true;
	}

	@Override
	protected boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getLeft() != null) && (aggregation.getRight() != null));
	}

	@Override
	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		addFieldSubsettingToIterator(scanner);
		IteratorSetting iteratorSettings = null;
		if (isAggregation()) {
			if (useWholeRowIterator()) {
				iteratorSettings = new IteratorSetting(
						QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
						QueryFilterIterator.QUERY_ITERATOR_NAME,
						WholeRowAggregationIterator.class);
			}
			else {
				iteratorSettings = new IteratorSetting(
						QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
						QueryFilterIterator.QUERY_ITERATOR_NAME,
						AggregationIterator.class);
			}
			iteratorSettings.addOption(
					AggregationIterator.ADAPTER_OPTION_NAME,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(aggregation.getLeft())));
			final Aggregation aggr = aggregation.getRight();
			iteratorSettings.addOption(
					AggregationIterator.AGGREGATION_OPTION_NAME,
					aggr.getClass().getName());
			iteratorSettings.addOption(
					AggregationIterator.CONSTRAINTS_OPTION_NAME,
					ByteArrayUtils.byteArrayToString((PersistenceUtils.toBinary(constraints))));
			iteratorSettings.addOption(
					AggregationIterator.INDEX_STRATEGY_OPTION_NAME,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexStrategy())));
			// the index model must be provided for the aggregation iterator to
			// deserialize each entry
			iteratorSettings.addOption(
					QueryFilterIterator.MODEL,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));
			// don't bother setting max decomposition because it is just the
			// default anyways
		}
		if ((distributableFilters != null) && !distributableFilters.isEmpty() && queryFiltersEnabled) {
			if (iteratorSettings == null) {
				if (useWholeRowIterator()) {
					iteratorSettings = new IteratorSetting(
							QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
							QueryFilterIterator.QUERY_ITERATOR_NAME,
							WholeRowQueryFilterIterator.class);
				}
				else {
					iteratorSettings = new IteratorSetting(
							QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
							QueryFilterIterator.QUERY_ITERATOR_NAME,
							QueryFilterIterator.class);
				}
			}
			DistributableQueryFilter filter;
			if (distributableFilters.size() > 1) {
				filter = new DistributableFilterList(
						distributableFilters);
			}
			else {
				filter = distributableFilters.get(0);
			}
			iteratorSettings.addOption(
					QueryFilterIterator.FILTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filter)));
			if (!iteratorSettings.getOptions().containsKey(
					QueryFilterIterator.MODEL)) {
				// it may already be added as an option if its an aggregation
				iteratorSettings.addOption(
						QueryFilterIterator.MODEL,
						ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));
			}
		}
		else if ((iteratorSettings == null) && useWholeRowIterator()) {
			// we have to at least use a whole row iterator
			iteratorSettings = new IteratorSetting(
					QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
					QueryFilterIterator.QUERY_ITERATOR_NAME,
					WholeRowIterator.class);
		}
		if (iteratorSettings != null) {
			scanner.addScanIterator(iteratorSettings);
		}
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
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
			return DataStoreUtils.constraintsToByteArrayRanges(
					constraints,
					index.getIndexStrategy(),
					MAX_RANGE_DECOMPOSITION,
					indexMetaData);
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
	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final ScannerBase scanner ) {
		if (isAggregation()) {
			// aggregate the stats to a single value here

			try {
				final Iterator<Entry<Key, Value>> it = scanner.iterator();
				Mergeable mergedAggregationResult = null;
				if (!it.hasNext()) {
					return Iterators.emptyIterator();
				}
				else {
					while (it.hasNext()) {
						final Entry<Key, Value> input = it.next();
						if (input.getValue() != null) {
							if (mergedAggregationResult == null) {
								mergedAggregationResult = PersistenceUtils.fromBinary(
										input.getValue().get(),
										Mergeable.class);
							}
							else {
								mergedAggregationResult.merge(PersistenceUtils.fromBinary(
										input.getValue().get(),
										Mergeable.class));
							}
						}
					}
				}
				return Iterators.singletonIterator(mergedAggregationResult);
			}
			finally {
				scanner.close();
			}
		}
		else {
			return super.initIterator(
					adapterStore,
					scanner);
		}
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
}
