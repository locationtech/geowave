package org.locationtech.geowave.core.store.adapter.statistics;

import java.util.Arrays;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;

/**
 * This is a marker class extending ByteArrayId that additionally provides type
 * checking with a generic.
 *
 * @param <R>
 *            The type of statistic
 */
abstract public class StatisticsType<R, B extends StatisticsQueryBuilder<R, B>> extends
		ByteArray implements
		Persistable
{
	private static final long serialVersionUID = 1L;

	public StatisticsType() {
		super();
	}

	public StatisticsType(
			final byte[] id ) {
		super(
				id);
	}

	public StatisticsType(
			final String id ) {
		super(
				id);
	}

	abstract public B newBuilder();

	@Override
	public byte[] toBinary() {
		return bytes;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		this.bytes = bytes;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		// If all we know is the name of the stat type,
		// but not the class we need to override equals on
		// the base statistics type so that the
		// class does not need to match
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof StatisticsType)) {
			return false;
		}
		final StatisticsType<?, ?> other = (StatisticsType<?, ?>) obj;
		return Arrays.equals(
				bytes,
				other.getBytes());
	}
}
