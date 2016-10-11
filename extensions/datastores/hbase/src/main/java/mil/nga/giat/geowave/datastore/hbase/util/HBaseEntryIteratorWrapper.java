package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.EntryIteratorWrapper;

public class HBaseEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(HBaseEntryIteratorWrapper.class);

	private byte[] fieldSubsetBitmask;

	private Integer bitPosition;
	private ByteArrayId skipUntilRow;

	private boolean reachedEnd = false;

	public HBaseEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final double[] maxResolutionSubsamplingPerDimension ) {
		this(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				null,
				fieldIds,
				maxResolutionSubsamplingPerDimension);
	}

	public HBaseEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final double[] maxResolutionSubsamplingPerDimension ) {
		super(
				true,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
		initializeBitPosition(maxResolutionSubsamplingPerDimension);
		if (fieldIds != null) {
			fieldSubsetBitmask = BitmaskUtils.generateFieldSubsetBitmask(
					index.getIndexModel(),
					fieldIds.getLeft(),
					fieldIds.getRight());
		}
	}

	@Override
	protected T decodeRow(
			final Object row,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final boolean wholeRowEncoding ) {
		Result result = null;
		try {
			result = (Result) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error("Row is not an HBase row Result.");
			return null;
		}

		if (passesResolutionSkippingFilter(result)) {
			return HBaseUtils.decodeRow(
					result,
					adapterStore,
					clientFilter,
					index,
					scanCallback,
					fieldSubsetBitmask);
		}
		return null;
	}

	private boolean passesResolutionSkippingFilter(
			final Result result ) {
		if ((reachedEnd == true) || ((skipUntilRow != null) && (skipUntilRow.compareTo(new ByteArrayId(
				result.getRow())) > 0))) {
			return false;
		}
		incrementSkipRow(result);
		return true;
	}

	private void incrementSkipRow(
			final Result result ) {
		if (bitPosition != null) {
			final int cardinality = bitPosition + 1;
			final byte[] rowCopy = new byte[(int) Math.ceil(cardinality / 8.0)];
			System.arraycopy(
					result.getRow(),
					0,
					rowCopy,
					0,
					rowCopy.length);
			// number of bits not used in the last byte
			int remainder = (8 - (cardinality % 8));
			if (remainder == 8) {
				remainder = 0;
			}

			final int numIncrements = (int) Math.pow(
					2,
					remainder);
			if (remainder > 0) {
				for (int i = 0; i < remainder; i++) {
					rowCopy[rowCopy.length - 1] |= (1 << (i));
				}
			}
			for (int i = 0; i < numIncrements; i++) {
				if (!ByteArrayUtils.increment(rowCopy)) {
					reachedEnd = true;
					return;
				}
			}
			skipUntilRow = new ByteArrayId(
					rowCopy);
		}
	}

	private void initializeBitPosition(
			final double[] maxResolutionSubsamplingPerDimension ) {
		if ((maxResolutionSubsamplingPerDimension != null) && (maxResolutionSubsamplingPerDimension.length > 0)) {
			bitPosition = (int) Math.round(IndexUtils.getDimensionalBitsUsed(
					index.getIndexStrategy(),
					maxResolutionSubsamplingPerDimension)
					+ (8 * index.getIndexStrategy().getByteOffsetFromDimensionalIndex()));
		}
	}

}
