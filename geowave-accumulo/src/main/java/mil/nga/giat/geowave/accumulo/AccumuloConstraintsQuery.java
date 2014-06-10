package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.DedupeFilter;
import mil.nga.giat.geowave.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.store.filter.FilterList;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

import com.google.common.collect.Iterators;

/**
 * This class represents basic numeric contraints applied to an Accumulo Query
 * 
 */
public class AccumuloConstraintsQuery extends
		AccumuloQuery
{
	protected final MultiDimensionalNumericData constraints;
	protected final List<QueryFilter> clientFilters;
	protected final List<DistributableQueryFilter> distributableFilters;

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
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters ) {
		this(
				null,
				index,
				constraints,
				queryFilters);
	}

	public AccumuloConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final MultiDimensionalNumericData constraints,
			final List<QueryFilter> queryFilters ) {
		super(
				adapterIds,
				index);
		this.constraints = constraints;
		final SplitFilterLists lists = splitList(queryFilters);
		clientFilters = lists.clientFilters;
		// add dedupe filters to the front of both lists so that the
		// de-duplication is performed before any more complex filtering
		// operations
		clientFilters.add(
				0,
				new DedupeFilter());
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
		if (constraints == null) {
			return null;
		}
		else {
			return index.getIndexStrategy().getQueryRanges(
					constraints);
		}
	}

	public CloseableIterator<Object> query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				limit);
		addScanIteratorSettings(scanner);
		Iterator<Object> it = new EntryIteratorWrapper(
				adapterStore,
				index,
				scanner.iterator(),
				new FilterList<QueryFilter>(
						clientFilters));
		if ((limit != null) && (limit >= 0)) {
			it = Iterators.limit(
					it,
					limit);
		}
		return new CloseableIteratorWrapper<Object>(
				new ScannerClosableWrapper(
						scanner),
				it);
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
