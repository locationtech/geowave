package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class HBaseMergingEntryIterator<T> extends
		HBaseEntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMergingEntryIterator.class);

	private final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters;
	private final Map<ByteArrayId, RowTransform> transforms;

	private Result peekedValue;

	public HBaseMergingEntryIterator(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ?> scanCallback,
			final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean hasSkippingFilter ) {
		super(
				dataStore,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback,
				fieldIds,
				maxResolutionSubsamplingPerDimension,
				true,
				hasSkippingFilter);
		this.mergingAdapters = mergingAdapters;
		transforms = new HashMap<ByteArrayId, RowTransform>();
	}

	@Override
	protected Object getNextEncodedResult() {

		// Get next result from scanner
		// We may have already peeked at it
		Result nextResult = null;
		if (peekedValue != null) {
			nextResult = peekedValue;
		}
		else {
			nextResult = (Result) scannerIt.next();
		}
		peekedValue = null;

		final GeoWaveRow rowId = new GeoWaveKeyImpl(
				nextResult.getRow());
		final ByteArrayId adapterId = new ByteArrayId(
				rowId.getAdapterId());
		final RowMergingDataAdapter mergingAdapter = mergingAdapters.get(adapterId);

		final ArrayList<Result> resultsToMerge = new ArrayList<Result>();

		if ((mergingAdapter != null) && (mergingAdapter.getTransform() != null)) {

			resultsToMerge.add(nextResult);

			// Peek ahead to see if it needs to be merged with the next result
			while (scannerIt.hasNext()) {
				peekedValue = (Result) scannerIt.next();
				final GeoWaveRow nextRowId = new GeoWaveKeyImpl(
						peekedValue.getRow());

				if (DataStoreUtils.rowIdsMatch(
						rowId,
						nextRowId)) {

					resultsToMerge.add(peekedValue);
					peekedValue = null;
				}
				else {
					if (resultsToMerge.size() > 1) {
						nextResult = mergeResults(
								mergingAdapter,
								resultsToMerge);
					}
					resultsToMerge.clear();
					return nextResult;
				}
			}
			// If the last results in the scanner are mergeable, merge them
			if (resultsToMerge.size() > 1) {
				nextResult = mergeResults(
						mergingAdapter,
						resultsToMerge);
			}
			resultsToMerge.clear();
		}

		return nextResult;
	}

	private Result mergeResults(
			final RowMergingDataAdapter mergingAdapter,
			final ArrayList<Result> resultsToMerge ) {

		Collections.sort(
				resultsToMerge,
				new Comparator<Result>() {
					@Override
					public int compare(
							final Result row1,
							final Result row2 ) {
						final ByteBuffer buf1 = ByteBuffer.wrap(new GeoWaveKeyImpl(
								row1.getRow()).getDataId());
						final ByteBuffer buf2 = ByteBuffer.wrap(new GeoWaveKeyImpl(
								row2.getRow()).getDataId());
						buf1.position(buf1.remaining() - DataStoreUtils.UNIQUE_ADDED_BYTES + 1);
						buf2.position(buf2.remaining() - DataStoreUtils.UNIQUE_ADDED_BYTES + 1);

						final long ts1 = buf1.getLong();
						final long ts2 = buf2.getLong();
						return Long.compare(
								ts2,
								ts1);
					}

				});

		final Iterator<Result> iter = resultsToMerge.iterator();
		Result mergedResult = iter.next();
		while (iter.hasNext()) {
			mergedResult = merge(
					mergingAdapter,
					mergedResult,
					iter.next());
		}

		return mergedResult;
	}

	private Result merge(
			final RowMergingDataAdapter mergingAdapter,
			final Result row,
			final Result rowToMerge ) {

		RowTransform transform = transforms.get(mergingAdapter.getAdapterId());
		if (transform == null) {
			transform = mergingAdapter.getTransform();
			// set strategy
			try {
				transform.initOptions(mergingAdapter.getOptions(null));
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to initialize merge strategy for adapter: " + mergingAdapter.getAdapterId(),
						e);
			}
			transforms.put(
					mergingAdapter.getAdapterId(),
					transform);
		}

		final Cell[] mergedCells = new Cell[rowToMerge.listCells().size()];

		int cellNum = 0;
		for (final Cell cell : row.listCells()) {
			final Cell cellToMerge = rowToMerge.listCells().get(
					cellNum);

			final Mergeable mergeable = transform.getRowAsMergeableObject(
					new ByteArrayId(
							CellUtil.cloneFamily(cell)),
					new ByteArrayId(
							CellUtil.cloneQualifier(cell)),
					CellUtil.cloneValue(cell));

			mergeable.merge(transform.getRowAsMergeableObject(
					new ByteArrayId(
							CellUtil.cloneFamily(cellToMerge)),
					new ByteArrayId(
							CellUtil.cloneQualifier(cellToMerge)),
					CellUtil.cloneValue(cellToMerge)));
			GeoWaveRow tempRow = new GeoWaveKeyImpl(
					row.getRow());
			mergedCells[cellNum] = CellUtil.createCell(
					new GeoWaveKeyImpl(
							DataStoreUtils.removeUniqueId(tempRow.getDataId()),
							tempRow.getAdapterId(),
							tempRow.getIndex(),
							tempRow.getNumberOfDuplicates()).getRowId(),
					CellUtil.cloneFamily(cell),
					CellUtil.cloneQualifier(cell),
					cell.getTimestamp(),
					cell.getTypeByte(),
					transform.getBinaryFromMergedObject(mergeable));

			cellNum++;
		}

		return Result.create(mergedCells);
	}

	@Override
	protected boolean hasNextScannedResult() {
		return (peekedValue != null) || scannerIt.hasNext();
	}

	@Override
	public void remove() {
		throw new NotImplementedException(
				"Transforming iterator cannot use remove()");
	}

}
