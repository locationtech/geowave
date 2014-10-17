package mil.nga.giat.geowave.accumulo.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.filter.DedupeFilter;
import mil.nga.giat.geowave.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.Query;

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
	protected final MultiDimensionalNumericData constraints;
	protected final List<DistributableQueryFilter> distributableFilters;

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
				null,
				new String[0]);
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
			final List<QueryFilter> queryFilters,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				authorizations);
		this.constraints = constraints;
		final SplitFilterLists lists = splitList(queryFilters);
		List<QueryFilter> clientFilters = lists.clientFilters;
		// add dedupe filters to the front of both lists so that the
		// de-duplication is performed before any more complex filtering
		// operations
		clientFilters.add(
				0,
				new DedupeFilter());
		super.setClientFilters(clientFilters);
		distributableFilters = lists.distributableFilters;
		distributableFilters.add(
				0,
				new DedupeFilter());

	}

	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		if ((distributableFilters != null) && !distributableFilters.isEmpty()) {
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
		if (constraints == null || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
													// positive infinity
		}
		else {
			return index.getIndexStrategy().getQueryRanges(
					constraints);
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
