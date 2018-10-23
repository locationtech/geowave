package org.locationtech.geowave.datastore.redis.util;

import java.time.Instant;

import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class GeoWaveRedisPersistedTimestampRow extends
		GeoWaveRedisPersistedRow
{
	private final long secondsSinceEpic;
	private final int nanoOfSecond;

	public GeoWaveRedisPersistedTimestampRow(
			final short numDuplicates,
			final byte[] dataId,
			final GeoWaveValue value,
			final Instant time ) {
		this(
				numDuplicates,
				dataId,
				value,
				time.getEpochSecond(),
				time.getNano());
	}

	public GeoWaveRedisPersistedTimestampRow(
			final short numDuplicates,
			final byte[] dataId,
			final GeoWaveValue value,
			final long secondsSinceEpic,
			final int nanoOfSecond ) {
		super(
				numDuplicates,
				dataId,
				value);
		this.secondsSinceEpic = secondsSinceEpic;
		this.nanoOfSecond = nanoOfSecond;
	}

	public long getSecondsSinceEpic() {
		return secondsSinceEpic;
	}

	public int getNanoOfSecond() {
		return nanoOfSecond;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = (prime * result) + nanoOfSecond;
		result = (prime * result) + (int) (secondsSinceEpic ^ (secondsSinceEpic >>> 32));
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveRedisPersistedTimestampRow other = (GeoWaveRedisPersistedTimestampRow) obj;
		if (nanoOfSecond != other.nanoOfSecond) {
			return false;
		}
		if (secondsSinceEpic != other.secondsSinceEpic) {
			return false;
		}
		return true;
	}
}
