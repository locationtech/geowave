package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.nio.ByteBuffer;
import java.time.Instant;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.threeten.extra.Interval;

abstract public class TimeRangeAggregation<P extends Persistable, T> implements
		Aggregation<P, Interval, T>
{

	protected long min = Long.MAX_VALUE;
	protected long max = Long.MIN_VALUE;

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public P getParameters() {
		return null;
	}

	@Override
	public void setParameters(
			final P parameters ) {}

	public boolean isSet() {
		if ((min == Long.MAX_VALUE) || (max == Long.MIN_VALUE)) {
			return false;
		}
		return true;
	}

	@Override
	public Interval getResult() {
		if (!isSet()) {
			return null;
		}
		return Interval.of(
				Instant.ofEpochMilli(min),
				Instant.ofEpochMilli(max));
	}

	@Override
	public Interval merge(
			final Interval result1,
			final Interval result2 ) {
		if (result1 == null) {
			return result2;
		}
		else if (result2 == null) {
			return result1;
		}
		final long min = Math.min(
				result1.getStart().toEpochMilli(),
				result1.getEnd().toEpochMilli());
		final long max = Math.max(
				result2.getStart().toEpochMilli(),
				result2.getEnd().toEpochMilli());
		return Interval.of(
				Instant.ofEpochMilli(min),
				Instant.ofEpochMilli(max));
	}

	@Override
	public byte[] resultToBinary(
			final Interval result ) {
		final ByteBuffer buffer = ByteBuffer.allocate(16);
		if (result == null) {
			buffer.putLong(Long.MAX_VALUE);
			buffer.putLong(Long.MIN_VALUE);
		}
		else {
			buffer.putLong(result.getStart().toEpochMilli());
			buffer.putLong(

			result.getEnd().toEpochMilli());
		}
		return buffer.array();
	}

	@Override
	public Interval resultFromBinary(
			final byte[] binary ) {
		final ByteBuffer buffer = ByteBuffer.wrap(binary);
		final long minTime = buffer.getLong();
		final long maxTime = buffer.getLong();
		if ((min == Long.MAX_VALUE) || (max == Long.MIN_VALUE)) {
			return null;
		}
		return Interval.of(
				Instant.ofEpochMilli(minTime),
				Instant.ofEpochMilli(maxTime));
	}

	@Override
	public void clearResult() {
		min = Long.MAX_VALUE;
		max = Long.MIN_VALUE;
	}

	@Override
	public void aggregate(
			final T entry ) {
		final Interval env = getInterval(entry);
		aggregate(env);
	}

	protected void aggregate(
			final Interval interval ) {
		if (interval != null) {
			min = Math.min(
					min,
					interval.getStart().toEpochMilli());
			max = Math.max(
					max,
					interval.getEnd().toEpochMilli());
		}
	}

	abstract protected Interval getInterval(
			final T entry );

}
