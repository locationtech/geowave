package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public class MaxDuplicatesStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"MAX_DUPLICATES");
	private static final ByteArrayId SEPARATOR = new ByteArrayId(
			"_");
	private static final byte[] STATS_ID_AND_SEPARATOR = ArrayUtils.addAll(
			STATS_ID.getBytes(),
			SEPARATOR.getBytes());
	private int maxDuplicates = 0;

	protected MaxDuplicatesStatistics() {
		super();
	}

	public int getEntriesWithDifferingFieldVisibilities() {
		return maxDuplicates;
	}

	private MaxDuplicatesStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statsId,
			final int maxDuplicates ) {
		super(
				dataAdapterId,
				statsId);
		this.maxDuplicates = maxDuplicates;
	}

	public MaxDuplicatesStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId ) {
		super(
				dataAdapterId,
				composeId(indexId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID_AND_SEPARATOR,
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new MaxDuplicatesStatistics<>(
				dataAdapterId,
				statisticsId,
				maxDuplicates);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = super.binaryBuffer(8);
		buf.putInt(maxDuplicates);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		maxDuplicates = buf.getInt();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			maxDuplicates = Math.max(
					maxDuplicates,
					kv.getNumberOfDuplicates());
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof MaxDuplicatesStatistics)) {
			maxDuplicates = Math.max(
					maxDuplicates,
					((MaxDuplicatesStatistics) merge).maxDuplicates);
		}
	}

}
