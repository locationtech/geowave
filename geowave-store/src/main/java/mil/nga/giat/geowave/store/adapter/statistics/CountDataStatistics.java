package mil.nga.giat.geowave.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.store.DeleteCallback;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;

public class CountDataStatistics<T> extends
		AbstractDataStatistics<T> implements
		IngestCallback<T>,
		DeleteCallback<T>
{
	public final static ByteArrayId STATS_ID = new ByteArrayId(
			"DATA_COUNT");

	private long count = Long.MIN_VALUE;

	protected CountDataStatistics() {
		super();
	}

	public CountDataStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId,
				STATS_ID);
	}

	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	public long getCount() {
		return count;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(count);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		count = buffer.getLong();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (!isSet()) {
			count = 0;
		}
		count += 1;
	}

	@Override
	public ByteArrayId getStatisticsId() {
		return STATS_ID;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if (!isSet()) {
			count = 0;
		}
		if ((statistics != null) && (statistics instanceof CountDataStatistics)) {
			@SuppressWarnings("unchecked")
			final CountDataStatistics<T> cStats = (CountDataStatistics<T>) statistics;
			if (cStats.isSet()) {
				count = count + cStats.count;
			}
		}
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (!isSet()) {
			count = 0;
		}
		count -= 1;

	}
}
