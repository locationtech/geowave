package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
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
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseFilteredIndexQuery.class);
	private boolean hasSkippingFilter = false;

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

	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		return internalQuery(
				operations,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit,
				true);
	}

	protected CloseableIterator<Object> internalQuery(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final boolean decodePersistenceEncoding ) {
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
			LOGGER.warn(
					"Unabe to check if " + StringUtils.stringFromBinary(index.getId().getBytes()) + " table exists",
					ex);
			return new CloseableIterator.Empty();
		}

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());

		final List<Iterator<Result>> resultsIterators = new ArrayList<Iterator<Result>>();
		final List<ResultScanner> results = new ArrayList<ResultScanner>();

		if (isBigtable()) {
			final List<Scan> scanners = getScannerList(limit);

			for (final Scan scanner : scanners) {
				try {
					final ResultScanner rs = operations.getScannedResults(
							scanner,
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
			}
		}
		else {
			final FilterList filterList = new FilterList();

			final Scan multiScanner = getMultiScanner(
					filterList,
					limit,
					maxResolutionSubsamplingPerDimension);

			if (isEnableCustomFilters()) {
				// Add skipping filter if requested
				hasSkippingFilter = false;
				if (maxResolutionSubsamplingPerDimension != null) {
					if (maxResolutionSubsamplingPerDimension.length != index
							.getIndexStrategy()
							.getOrderedDimensionDefinitions().length) {
						LOGGER.warn("Unable to subsample for table '" + tableName + "'. Subsample dimensions = "
								+ maxResolutionSubsamplingPerDimension.length + " when indexed dimensions = "
								+ index.getIndexStrategy().getOrderedDimensionDefinitions().length);
					}
					else {
						final int cardinalityToSubsample = IndexUtils.getBitPositionFromSubsamplingArray(
								index.getIndexStrategy(),
								maxResolutionSubsamplingPerDimension);

						final FixedCardinalitySkippingFilter skippingFilter = new FixedCardinalitySkippingFilter(
								cardinalityToSubsample);
						filterList.addFilter(skippingFilter);
						hasSkippingFilter = true;
					}
				}

				// Add distributable filters if requested, this has to be last
				// in the filter list for the dedupe filter to work correctly
				final List<DistributableQueryFilter> distFilters = getDistributableFilters();
				if ((distFilters != null) && !distFilters.isEmpty()) {
					final HBaseDistributableFilter hbdFilter = new HBaseDistributableFilter();
					hbdFilter.init(
							distFilters,
							index.getIndexModel());

					filterList.addFilter(hbdFilter);
				}
				else {
					final List<MultiDimensionalCoordinateRangesArray> coords = getCoordinateRanges();
					if ((coords != null) && !coords.isEmpty()) {
						final HBaseNumericIndexStrategyFilter numericIndexFilter = new HBaseNumericIndexStrategyFilter(
								index.getIndexStrategy(),
								coords.toArray(new MultiDimensionalCoordinateRangesArray[] {}));
						filterList.addFilter(numericIndexFilter);
					}
				}
			}

			if (!filterList.getFilters().isEmpty()) {
				multiScanner.setFilter(filterList);
			}

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
				LOGGER.warn(
						"Could not get the results from scanner",
						e);
			}
		}

		if (results.iterator().hasNext()) {
			Iterator it = initIterator(
					adapterStore,
					Iterators.concat(resultsIterators.iterator()),
					maxResolutionSubsamplingPerDimension,
					decodePersistenceEncoding);

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

	private boolean isEnableCustomFilters() {
		return (options != null && options.isEnableCustomFilters());
	}

	private boolean isBigtable() {
		return (options != null && options.isBigTable());
	}

	// Bigtable does not support MultiRowRangeFilters. This method returns a
	// single scan per range
	protected List<Scan> getScannerList(
			final Integer limit ) {

		List<ByteArrayRange> ranges = getRanges();
		if ((ranges == null) || ranges.isEmpty()) {
			ranges = Collections.singletonList(new ByteArrayRange(
					null,
					null));
		}

		final List<Scan> scanners = new ArrayList<Scan>();
		if ((ranges != null) && (ranges.size() > 0)) {
			for (final ByteArrayRange range : ranges) {
				final Scan scanner = createStandardScanner(limit);

				if (range.getStart() != null) {
					scanner.setStartRow(range.getStart().getBytes());
					if (!range.isSingleValue()) {
						scanner.setStopRow(HBaseUtils.getNextPrefix(range.getEnd().getBytes()));
					}
					else {
						scanner.setStopRow(HBaseUtils.getNextPrefix(range.getStart().getBytes()));
					}
				}

				scanners.add(scanner);
			}
		}

		return scanners;
	}

	// Default (not Bigtable) case - use a single multi-row-range filter
	protected Scan getMultiScanner(
			final FilterList filterList,
			final Integer limit,
			final double[] maxResolutionSubsamplingPerDimension ) {
		// Single scan w/ multiple ranges
		final Scan multiScanner = createStandardScanner(limit);

		final List<ByteArrayRange> ranges = getRanges();

		final MultiRowRangeFilter filter = getMultiRowRangeFilter(ranges);
		if (filter != null) {
			filterList.addFilter(filter);

			final List<RowRange> rowRanges = filter.getRowRanges();
			multiScanner.setStartRow(rowRanges.get(
					0).getStartRow());

			final RowRange stopRowRange = rowRanges.get(rowRanges.size() - 1);
			byte[] stopRowExclusive;
			if (stopRowRange.isStopRowInclusive()) {
				// because the end is always exclusive, to make an inclusive
				// stop row into exlusive all we need to do is add a traling 0
				stopRowExclusive = new byte[stopRowRange.getStopRow().length + 1];

				System.arraycopy(
						stopRowRange.getStopRow(),
						0,
						stopRowExclusive,
						0,
						stopRowExclusive.length - 1);
			}
			else {
				stopRowExclusive = stopRowRange.getStopRow();
			}
			multiScanner.setStopRow(stopRowExclusive);
		}

		return multiScanner;
	}

	protected Scan createStandardScanner(
			final Integer limit ) {
		final Scan scanner = new Scan();

		// Performance tuning per store options
		scanner.setCaching(getScanCacheSize());
		scanner.setCacheBlocks(isEnableBlockCache());

		// Only return the most recent version
		scanner.setMaxVersions(1);

		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId adapterId : adapterIds) {
				scanner.addFamily(adapterId.getBytes());
			}
		}

		if ((limit != null) && (limit > 0) && (limit < scanner.getBatch())) {
			scanner.setBatch(limit);
		}

		return scanner;
	}

	private int getScanCacheSize() {
		if (options != null) {
			if (options.getScanCacheSize() != HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING) {
				return options.getScanCacheSize();
			}
		}

		// Need to get default from config.
		return 10000;
	}

	private boolean isEnableBlockCache() {
		if (options != null) {
			return options.isEnableBlockCache();
		}

		return true;
	}

	protected MultiRowRangeFilter getMultiRowRangeFilter(
			final List<ByteArrayRange> ranges ) {
		// create the multi-row filter
		final List<RowRange> rowRanges = new ArrayList<RowRange>();
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
							false);

					rowRanges.add(rowRange);
				}
			}
		}

		// Create the multi-range filter
		try {
			return new MultiRowRangeFilter(
					rowRanges);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating range filter.",
					e);
		}
		return null;
	}

	// Override this (see HBaseConstraintsQuery)
	protected List<DistributableQueryFilter> getDistributableFilters() {
		return null;
	}

	// Override this (see HBaseConstraintsQuery)
	protected List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return null;
	}

	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<Result> resultsIterator,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
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
			if ((adapter instanceof RowMergingDataAdapter)
					&& (((RowMergingDataAdapter) adapter).getTransform() != null)) {
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
					maxResolutionSubsamplingPerDimension,
					decodePersistenceEncoding,
					hasSkippingFilter);
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
					maxResolutionSubsamplingPerDimension,
					hasSkippingFilter);
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
