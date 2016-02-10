/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper.MultiScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloFilteredIndexQuery </code>
 */
public abstract class HBaseFilteredIndexQuery extends
		HBaseQuery
{

	protected final ScanCallback<?> scanCallback;
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(
			HBaseFilteredIndexQuery.class);
	private Collection<String> fieldIds = null;

	public HBaseFilteredIndexQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				authorizations);
		this.scanCallback = scanCallback;
	}

	protected void setClientFilters(
			final List<QueryFilter> clientFilters ) {
		this.clientFilters = clientFilters;
	}

	public void setFieldIds(
			final Collection<String> fieldIds ) {
		this.fieldIds = fieldIds;
	}

	@SuppressWarnings("rawtypes")
	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		try {
			if (!operations.tableExists(
					StringUtils.stringFromBinary(
							index.getId().getBytes()))) {
				LOGGER.warn(
						"Table does not exist " + StringUtils.stringFromBinary(
								index.getId().getBytes()));
				return new CloseableIterator.Empty();
			}
		}
		catch (final IOException ex) {
			LOGGER.warn(
					"Unabe to check if " + StringUtils.stringFromBinary(
							index.getId().getBytes()) + " table exists");
			return new CloseableIterator.Empty();
		}
		final String tableName = StringUtils.stringFromBinary(
				index.getId().getBytes());

		final List<Filter> distributableFilters = getDistributableFilter();

		CloseableIterator<DataAdapter<?>> adapters = null;
		if ((fieldIds != null) && !fieldIds.isEmpty()) {
			adapters = adapterStore.getAdapters();
		}

		final List<Scan> scanners = getScanners(
				limit,
				distributableFilters,
				adapters);

		final List<Iterator<Result>> resultsIterators = new ArrayList<Iterator<Result>>();
		final List<ResultScanner> results = new ArrayList<ResultScanner>();
		try {

			// TODO Consider parallelization as list of scanners can be long and
			// getScannedResults might be slow?
			for (final Scan scanner : scanners) {
				final ResultScanner rs = operations.getScannedResults(
						scanner,
						tableName);
				final Iterator<Result> it = rs.iterator();
				if ((rs != null) && it.hasNext()) {
					resultsIterators.add(
							it);
					results.add(
							rs);
				}
			}

			if (results.iterator().hasNext()) {
				Iterator it = initIterator(
						adapterStore,
						Iterators.concat(
								resultsIterators.iterator()));

				if ((limit != null) && (limit > 0)) {
					it = Iterators.limit(
							it,
							limit);
				}
				return new HBaseCloseableIteratorWrapper(
						new MultiScannerClosableWrapper(
								results),
						it);
			}
			else {
				LOGGER.error(
						"Results were empty");
				return null;
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Could not get the results from scanner");
			return null;
		}

		// final Scan scanner = getScanner(
		// limit,
		// distributableFilters,
		// adapters);
		// try {
		// final ResultScanner results = operations.getScannedResults(
		// scanner,
		// tableName);
		// Iterator<Result> it = results.iterator();
		//
		// if (it.hasNext()) {
		// Iterator geoWaveIt = initIterator(
		// adapterStore,
		// it);
		// if ((limit != null) && (limit > 0)) {
		// geoWaveIt = Iterators.limit(
		// geoWaveIt,
		// limit);
		// }
		// return new HBaseCloseableIteratorWrapper(
		// new ScannerClosableWrapper(
		// results),
		// geoWaveIt);
		// }
		// else {
		// LOGGER.error(
		// "Results were empty");
		// return null;
		// }
		// }
		// catch (final IOException e) {
		// LOGGER.error(
		// "Could not get the results from scanner");
		// return null;
		// }

	}

	protected abstract List<Filter> getDistributableFilter();

	private byte[] calculateTheClosestNextRowKeyForPrefix(
			final byte[] rowKeyPrefix ) {
		// Essentially we are treating it like an 'unsigned very very long' and
		// doing +1 manually.
		// Search for the place where the trailing 0xFFs start
		int offset = rowKeyPrefix.length;
		while (offset > 0) {
			if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
				break;
			}
			offset--;
		}

		if (offset == 0) {
			// We got an 0xFFFF... (only FFs) stopRow value which is
			// the last possible prefix before the end of the table.
			// So set it to stop at the 'end of the table'
			return HConstants.EMPTY_END_ROW;
		}

		// Copy the right length of the original
		final byte[] newStopRow = Arrays.copyOfRange(
				rowKeyPrefix,
				0,
				offset);
		// And increment the last one
		newStopRow[newStopRow.length - 1]++;
		return newStopRow;
	}

	private List<Scan> getScanners(
			final Integer limit,
			final List<Filter> distributableFilters,
			final CloseableIterator<DataAdapter<?>> adapters ) {
		FilterList filterList = null;
		if ((distributableFilters != null) && (distributableFilters.size() > 0)) {
			filterList = new FilterList();
			for (final Filter filter : distributableFilters) {
				filterList.addFilter(
						filter);
			}
		}
		final List<ByteArrayRange> ranges = getRanges();
		final List<Scan> scanners = new ArrayList<Scan>();
		if ((ranges != null) && (ranges.size() > 0)) {

			for (final ByteArrayRange range : ranges) {

				final Scan scanner = new Scan();

				if ((adapterIds != null) && !adapterIds.isEmpty()) {
					for (final ByteArrayId adapterId : adapterIds) {
						scanner.addFamily(
								adapterId.getBytes());
					}
				}

				scanner.setStartRow(
						range.getStart().getBytes());
				if (!range.isSingleValue()) {
					scanner.setStopRow(
							calculateTheClosestNextRowKeyForPrefix(
									range.getEnd().getBytes()));
				}

				scanner.setFilter(
						filterList);

				// a subset of fieldIds is being requested
				if ((fieldIds != null) && !fieldIds.isEmpty()) {
					// configure scanner to fetch only the fieldIds specified
					handleSubsetOfFieldIds(
							scanner,
							adapters);
				}

				if ((limit != null) && (limit > 0) && (limit < scanner.getBatch())) {
					scanner.setBatch(
							limit);
				}

				scanners.add(
						scanner);
			}
		}

		return scanners;
	}

	private Scan getScanner(
			final Integer limit,
			final List<Filter> distributableFilters,
			final CloseableIterator<DataAdapter<?>> adapters ) {

		final List<ByteArrayRange> ranges = getRanges();
		final Scan scanner = new Scan();

		if ((ranges != null) && (ranges.size() == 0)) {

			final ByteArrayRange range = ranges.get(
					0);

			scanner.setStartRow(
					range.getStart().getBytes());
			if (!range.isSingleValue()) {
				scanner.setStopRow(
						calculateTheClosestNextRowKeyForPrefix(
								range.getEnd().getBytes()));
			}
		}
		else if (ranges != null) {
			ByteArrayId minStart = null;
			ByteArrayId maxEnd = null;
			final List<RowRange> rowRanges = new ArrayList<RowRange>();
			for (final ByteArrayRange range : ranges) {
				if ((minStart == null) || (range.getStart().compareTo(
						minStart) < 0)) {
					minStart = range.getStart();
				}
				if ((maxEnd == null) || (range.getEnd().compareTo(
						maxEnd) > 0)) {
					maxEnd = range.getEnd();
				}
				rowRanges.add(
						new RowRange(
								range.getStart().getBytes(),
								true,
								calculateTheClosestNextRowKeyForPrefix(
										range.getEnd().getBytes()),
								false));
			}
			scanner.setStartRow(
					minStart.getBytes());
			scanner.setStopRow(
					calculateTheClosestNextRowKeyForPrefix(
							maxEnd.getBytes()));
			try {
				final MultiRowRangeFilter filter = new MultiRowRangeFilter(
						rowRanges.subList(
								0,
								15));
				scanner.setFilter(
						filter);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Failed to instantiate row range filter. " + e);
				e.printStackTrace();
			}
		}

		if ((limit != null) && (limit > 0) && (limit < scanner.getBatch())) {
			scanner.setBatch(
					limit);
		}

		// a subset of fieldIds is being requested
		if ((fieldIds != null) && !fieldIds.isEmpty()) {
			// configure scanner to fetch only the fieldIds specified
			handleSubsetOfFieldIds(
					scanner,
					adapters);
		}

		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId adapterId : adapterIds) {
				scanner.addFamily(
						adapterId.getBytes());
			}
		}

		return scanner;
	}

	private void handleSubsetOfFieldIds(
			final Scan scanner,
			final CloseableIterator<DataAdapter<?>> dataAdapters ) {

		final Set<ByteArrayId> uniqueDimensions = new HashSet<>();
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : index.getIndexModel().getDimensions()) {
			uniqueDimensions.add(
					dimension.getFieldId());
		}

		while (dataAdapters.hasNext()) {

			// dimension fields must be included
			final DataAdapter<?> next = dataAdapters.next();
			for (final ByteArrayId dimension : uniqueDimensions) {
				scanner.addColumn(
						next.getAdapterId().getBytes(),
						dimension.getBytes());
			}

			// configure scanner to fetch only the specified fieldIds
			for (final String fieldId : fieldIds) {
				scanner.addColumn(
						next.getAdapterId().getBytes(),
						StringUtils.stringToBinary(
								fieldId));
			}
		}

		try {
			dataAdapters.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}
	}

	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<Result> resultsIterator ) {
		// TODO Fix #406 Since currently we are not supporting server side
		// iterator/coprocessors, we also cannot run
		// server side filters and hence they have to run on clients itself. So
		// need to add server side filters also in list of client filters.
		final List<QueryFilter> filters = getAllFiltersList();
		return new HBaseEntryIteratorWrapper(
				adapterStore,
				index,
				resultsIterator,
				new mil.nga.giat.geowave.core.store.filter.FilterList<QueryFilter>(
						filters),
				scanCallback);
	}

	protected List<QueryFilter> getAllFiltersList() {
		// This method is so that it can be overridden to also add distributed
		// filter list
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.addAll(
				clientFilters);
		return filters;
	}
}
