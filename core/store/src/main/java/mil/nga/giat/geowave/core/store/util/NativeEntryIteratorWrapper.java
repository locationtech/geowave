package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class NativeEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final byte[] fieldSubsetBitmask;
	private final boolean decodePersistenceEncoding;
	private Integer bitPosition = null;
	private ByteArrayId skipUntilRow;
	private boolean reachedEnd = false;

	public NativeEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
			final byte[] fieldSubsetBitmask,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		super(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
		this.fieldSubsetBitmask = fieldSubsetBitmask;
		this.decodePersistenceEncoding = decodePersistenceEncoding;

		initializeBitPosition(maxResolutionSubsamplingPerDimension);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected T decodeRow(
			final GeoWaveRow row,
			final QueryFilter clientFilter,
			final PrimaryIndex index ) {
		if (bitPosition == null || passesSkipFilter(row)) {
			return (T) BaseDataStoreUtils.decodeRow(
					row,
					clientFilter,
					null,
					adapterStore,
					index,
					scanCallback,
					fieldSubsetBitmask,
					decodePersistenceEncoding);
		}

		return null;
	}

	private boolean passesSkipFilter(
			final GeoWaveRow row ) {
		if ((reachedEnd == true) || ((skipUntilRow != null) && (skipUntilRow.compareTo(new ByteArrayId(
				GeoWaveKey.getCompositeId(row))) > 0))) {
			return false;
		}

		incrementSkipRow(row);

		return true;
	}

	private void incrementSkipRow(
			final GeoWaveRow row ) {
		if (bitPosition != null) {
			final byte[] nextRow = IndexUtils.getNextRowForSkip(
					GeoWaveKey.getCompositeId(row),
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
