package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

/**
 * This class represents basic numeric contraints applied to an Accumulo Query
 * 
 */
public class AccumuloConstraintsQuery extends
		AccumuloFilteredIndexQuery
{
	private static final int MAX_RANGE_DECOMPOSITION = 5000;
	protected final MultiDimensionalNumericData constraints;
	protected final List<DistributableQueryFilter> distributableFilters;
	protected boolean queryFiltersEnabled;

	public AccumuloConstraintsQuery(
			final Index index,
			final Query query ) {
		this(
				null,
				index,
				query.getIndexConstraints(index.getIndexStrategy()),
				query.createFilters(index.getIndexModel()),
				new String[0]);
		if (!query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
	}

	public AccumuloConstraintsQuery(
			final Index index ) {
		this(
				null,
				index);
	}

	public AccumuloConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index ) {
		this(
				adapterIds,
				index,
				null,
				null);
	}

	public AccumuloConstraintsQuery(
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

	public AccumuloConstraintsQuery(
			final Index index,
			final MultiDimensionalNumericData constraints,
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
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters ) {
		this(
				adapterIds,
				index,
				constraints,
				queryFilters,
				null,
				null,
				new String[0]);

	}

	public AccumuloConstraintsQuery(
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

	public AccumuloConstraintsQuery(
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

	@Override
	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		if ((distributableFilters != null) && !distributableFilters.isEmpty() && queryFiltersEnabled) {
			final IteratorSetting iteratorSettings = new IteratorSetting(
					QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
					QueryFilterIterator.QUERY_ITERATOR_NAME,
					QueryFilterIterator.class);
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
		else {
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
		return AccumuloUtils.constraintsToByteArrayRanges(
				constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION);
	}

	public boolean isQueryFiltersEnabled() {
		return queryFiltersEnabled;
	}

	public void setQueryFiltersEnabled(
			boolean queryFiltersEnabled ) {
		this.queryFiltersEnabled = queryFiltersEnabled;
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
