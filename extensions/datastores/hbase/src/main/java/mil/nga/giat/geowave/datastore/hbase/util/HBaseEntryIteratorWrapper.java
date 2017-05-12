package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
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
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseEntryIteratorWrapper.class);
	private boolean decodePersistenceEncoding = true;

	private byte[] fieldSubsetBitmask;

	private Integer bitPosition;
	private ByteArrayId skipUntilRow;

	private boolean reachedEnd = false;
	private boolean hasSkippingFilter = false;

	public HBaseEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding,
			final boolean hasSkippingFilter ) {
		this(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				null,
				fieldIds,
				maxResolutionSubsamplingPerDimension,
				decodePersistenceEncoding,
				hasSkippingFilter);
	}

	public HBaseEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding,
			final boolean hasSkippingFilter ) {
		super(
				true,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
		this.decodePersistenceEncoding = decodePersistenceEncoding;
		this.hasSkippingFilter = hasSkippingFilter;

		if (!this.hasSkippingFilter) {
			initializeBitPosition(maxResolutionSubsamplingPerDimension);
		}
		else {
			bitPosition = null;
		}

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
			LOGGER.error(
					"Row is not an HBase row Result.",
					e);
			return null;
		}

		if (passesResolutionSkippingFilter(result)) {
			return HBaseUtils.decodeRow(
					result,
					adapterStore,
					clientFilter,
					index,
					scanCallback,
					fieldSubsetBitmask,
					decodePersistenceEncoding);
		}
		return null;
	}

	private boolean passesResolutionSkippingFilter(
			final Result result ) {
		if (hasSkippingFilter) {
			return true;
		}

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
			final byte[] nextRow = IndexUtils.getNextRowForSkip(
					result.getRow(),
					bitPosition);

			if (nextRow == null) {
				reachedEnd = true;
			}
			else {
				skipUntilRow = new ByteArrayId(
						nextRow);
			}
		}
	}

	private void initializeBitPosition(
			final double[] maxResolutionSubsamplingPerDimension ) {
		if ((maxResolutionSubsamplingPerDimension != null) && (maxResolutionSubsamplingPerDimension.length > 0)) {
			bitPosition = IndexUtils.getBitPositionFromSubsamplingArray(
					index.getIndexStrategy(),
					maxResolutionSubsamplingPerDimension);
		}
	}

}
