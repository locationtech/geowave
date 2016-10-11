package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.FilteredIndexQuery;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.MultiScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.MergingEntryIterator;

public abstract class HBaseFilteredIndexQuery extends
		HBaseQuery implements
		FilteredIndexQuery
{

	protected final ScanCallback<?> scanCallback;
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(HBaseFilteredIndexQuery.class);

	public HBaseFilteredIndexQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final ScanCallback<?> scanCallback,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				fieldIds,
				authorizations);
		this.scanCallback = scanCallback;
	}

	@Override
	public void setClientFilters(
			final List<QueryFilter> clientFilters ) {
		this.clientFilters = clientFilters;
	}

	private boolean validateAdapters(
			final BasicHBaseOperations operations )
			throws IOException {
		if ((adapterIds == null) || adapterIds.isEmpty()) {
			return true;
		}
		final Iterator<ByteArrayId> i = adapterIds.iterator();
		while (i.hasNext()) {
			final ByteArrayId adapterId = i.next();
			if (!operations.columnFamilyExists(
					index.getId().getString(),
					adapterId.getString())) {
				i.remove();
			}
		}
		if (adapterIds.isEmpty()) {
			return false;
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		try {
			if (!validateAdapters(operations)) {
				LOGGER.warn("Query contains no valid adapters.");
				return new CloseableIterator.Empty();
			}
			if (!operations.tableExists(StringUtils.stringFromBinary(index.getId().getBytes()))) {
				LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
				return new CloseableIterator.Empty();
			}
		}
		catch (final IOException ex) {
			LOGGER
					.warn("Unabe to check if " + StringUtils.stringFromBinary(index.getId().getBytes())
							+ " table exists");
			return new CloseableIterator.Empty();
		}

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());

		final List<Filter> distributableFilters = getDistributableFilter();

		final Scan multiScanner = getMultiScanner(
				limit,
				distributableFilters);

		final List<Iterator<Result>> resultsIterators = new ArrayList<Iterator<Result>>();
		final List<ResultScanner> results = new ArrayList<ResultScanner>();

		try {
			final ResultScanner rs = operations.getScannedResults(
					multiScanner,
					tableName,
					authorizations);

			if (rs != null) {
				results.add(rs);
				final Iterator<Result> it = rs.iterator();
				if (it.hasNext()) {
					resultsIterators.add(it);
				}
			}
		}
		catch (final IOException e) {
			LOGGER.warn("Could not get the results from scanner " + e);
		}

		if (results.iterator().hasNext()) {
			Iterator it = initIterator(
					adapterStore,
					Iterators.concat(resultsIterators.iterator()),
					maxResolutionSubsamplingPerDimension);

			if ((limit != null) && (limit > 0)) {
				it = Iterators.limit(
						it,
						limit);
			}

			return new CloseableIteratorWrapper(
					new MultiScannerClosableWrapper(
							results),
					it);
		}

		LOGGER.error("Results were empty");
		return new CloseableIterator.Empty();
	}

	protected abstract List<Filter> getDistributableFilter();

	// experiment to test a single multi-scanner vs multiple single-range
	// scanners
	protected Scan getMultiScanner(
			final Integer limit,
			final List<Filter> distributableFilters ) {
		// Single scan w/ multiple ranges
		final Scan scanner = new Scan();

		// Performance recommendations
		scanner.setCaching(10000);
		scanner.setCacheBlocks(true);

		final FilterList filterList = new FilterList();

		// Add server-side filters (currently not implemented)
		if ((distributableFilters != null) && (!distributableFilters.isEmpty())) {
			for (final Filter filter : distributableFilters) {
				filterList.addFilter(filter);
			}
		}

		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId adapterId : adapterIds) {
				scanner.addFamily(adapterId.getBytes());
			}
		}

		// create the multi-row filter
		final List<RowRange> rowRanges = new ArrayList<RowRange>();

		final List<ByteArrayRange> ranges = getRanges();
		if ((ranges == null) || ranges.isEmpty()) {
			rowRanges.add(new RowRange(
					HConstants.EMPTY_BYTE_ARRAY,
					true,
					HConstants.EMPTY_BYTE_ARRAY,
					false));
		}
		else {
			for (final ByteArrayRange range : ranges) {
				if (range.getStart() != null) {
					final byte[] startRow = range.getStart().getBytes();
					byte[] stopRow;
					if (!range.isSingleValue()) {
						stopRow = HBaseUtils.getNextPrefix(range.getEnd().getBytes());
					}
					else {
						stopRow = HBaseUtils.getNextPrefix(range.getStart().getBytes());
					}

					final RowRange rowRange = new RowRange(
							startRow,
							true,
							stopRow,
							true);

					rowRanges.add(rowRange);
				}
			}
		}

		// Create the multi-range filter
		try {
			final Filter filter = new MultiRowRangeFilter(
					rowRanges);

			filterList.addFilter(filter);
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		// Set the filter list for the scan and return the scan list (with the
		// single multi-range scan)
		scanner.setFilter(filterList);
		scanner.setMaxVersions(1);
		// Only return the most recent version
		scanner.setMaxVersions(1);

		return scanner;
	}

	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<Result> resultsIterator,
			final double[] maxResolutionSubsamplingPerDimension ) {
		// TODO Since currently we are not supporting server side
		// iterator/coprocessors, we also cannot run
		// server side filters and hence they have to run on clients itself. So
		// need to add server side filters also in list of client filters.
		final List<QueryFilter> filters = getAllFiltersList();
		final QueryFilter queryFilter = filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0)
				: new mil.nga.giat.geowave.core.store.filter.FilterList<QueryFilter>(
						filters);

		final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters = new HashMap<ByteArrayId, RowMergingDataAdapter>();
		for (final ByteArrayId adapterId : adapterIds) {
			final DataAdapter adapter = adapterStore.getAdapter(adapterId);
			if (adapter instanceof RowMergingDataAdapter && ((RowMergingDataAdapter) adapter).getTransform() != null) {
				mergingAdapters.put(
						adapterId,
						(RowMergingDataAdapter) adapter);
			}
		}

		if (mergingAdapters.isEmpty()) {
			return new HBaseEntryIteratorWrapper(
					adapterStore,
					index,
					resultsIterator,
					queryFilter,
					scanCallback,
					fieldIds,
					maxResolutionSubsamplingPerDimension);
		}
		else {
			return new MergingEntryIterator(
					adapterStore,
					index,
					resultsIterator,
					queryFilter,
					scanCallback,
					mergingAdapters,
					fieldIds,
					maxResolutionSubsamplingPerDimension);
		}
	}

	protected List<QueryFilter> getAllFiltersList() {
		// This method is so that it can be overridden to also add distributed
		// filter list
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.addAll(clientFilters);
		return filters;
	}
}
