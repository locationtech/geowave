package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.Iterators;

/**
 * This class represents basic numeric contraints applied to an Accumulo Query
 * 
 */
public class AccumuloConstraintsQuery extends
		AccumuloFilteredIndexQuery
{
	private static final int MAX_RANGE_DECOMPOSITION = 5000;
	protected final List<MultiDimensionalNumericData> constraints;
	protected final List<DistributableQueryFilter> distributableFilters;
	protected boolean queryFiltersEnabled;

	protected final Pair<DataAdapter<?>, Aggregation<?>> aggregation;

	public AccumuloConstraintsQuery(
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

	public AccumuloConstraintsQuery(
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

	public AccumuloConstraintsQuery(
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

	public AccumuloConstraintsQuery(
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

	public AccumuloConstraintsQuery(
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

	@Override
	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		if ((distributableFilters != null) && !distributableFilters.isEmpty() && queryFiltersEnabled) {

			final IteratorSetting iteratorSettings;
			if (isAggregation()) {
				iteratorSettings = new IteratorSetting(
						QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
						QueryFilterIterator.QUERY_ITERATOR_NAME,
						AggregationIterator.class);
				iteratorSettings.addOption(
						AggregationIterator.ADAPTER_OPTION_NAME,
						ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(aggregation.getLeft())));
				iteratorSettings.addOption(
						AggregationIterator.AGGREGATION_OPTION_NAME,
						ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(aggregation.getRight())));
			}
			else {
				iteratorSettings = new IteratorSetting(
						QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
						QueryFilterIterator.QUERY_ITERATOR_NAME,
						QueryFilterIterator.class);
			}
			final DistributableQueryFilter filterList = new DistributableFilterList(
					distributableFilters);
			iteratorSettings.addOption(
					QueryFilterIterator.FILTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(filterList)));
			iteratorSettings.addOption(
					QueryFilterIterator.MODEL,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel())));
			scanner.addScanIterator(iteratorSettings);
		}
		else

		{
			// we have to at least use a whole row iterator
			final IteratorSetting iteratorSettings = new IteratorSetting(
					QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
					QueryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);
		}

	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		return DataStoreUtils.constraintsToByteArrayRanges(
				constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION);
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

			final Iterator<Entry<Key, Value>> it = scanner.iterator();

			Aggregation mergedAggregation = null;
			if (!it.hasNext()) {
				mergedAggregation = aggregation.getRight();
			}
			else {
				while (it.hasNext()) {
					final Entry<Key, Value> input = it.next();
					if (input.getValue() != null) {
						if (mergedAggregation == null) {
							mergedAggregation = PersistenceUtils.fromBinary(
									input.getValue().get(),
									Aggregation.class);
						}
						else {
							mergedAggregation.merge(PersistenceUtils.fromBinary(
									input.getValue().get(),
									Aggregation.class));
						}
					}
				}
			}
			return Iterators.singletonIterator(mergedAggregation);
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
