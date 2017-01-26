package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class MaxDuplicatesStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"MAX_DUPLICATES");
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
			final ByteArrayId statisticsId ) {
		super(
				dataAdapterId,
				composeId(statisticsId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId statisticsId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						ArrayUtils.addAll(
								STATS_TYPE.getBytes(),
								STATS_SEPARATOR.getBytes()),
						statisticsId.getBytes()));
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
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		maxDuplicates = Math.max(
				maxDuplicates,
				entryInfo.getRowIds().size() - 1);
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
