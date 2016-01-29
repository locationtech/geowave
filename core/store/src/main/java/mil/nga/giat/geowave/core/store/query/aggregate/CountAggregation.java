package mil.nga.giat.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.Mergeable;

public class CountAggregation<T> implements
		Aggregation<T>
{
	private long count = Long.MIN_VALUE;

	public CountAggregation() {}

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
	public void merge(
			final Mergeable statistics ) {
		if (!isSet()) {
			count = 0;
		}
		if ((statistics != null) && (statistics instanceof CountAggregation)) {
			@SuppressWarnings("unchecked")
			final CountAggregation<T> cStats = (CountAggregation<T>) statistics;
			if (cStats.isSet()) {
				count = count + cStats.count;
			}
		}
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"count[count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public void aggregate(
			final T entry ) {
		if (!isSet()) {
			count = 0;
		}
		count += 1;

	}
}
