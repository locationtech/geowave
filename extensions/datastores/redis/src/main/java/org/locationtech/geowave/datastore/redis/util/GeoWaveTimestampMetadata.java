package org.locationtech.geowave.datastore.redis.util;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

/**
 * To guarantee uniqueness for metadata entries, we are adding a timestamp to
 * the persistence format
 *
 */
public class GeoWaveTimestampMetadata extends
		GeoWaveMetadata
{
	private final long millisFromEpoch;

	public GeoWaveTimestampMetadata(
			final GeoWaveMetadata md,
			final long millisFromEpoch ) {
		this(
				md.getPrimaryId(),
				md.getSecondaryId(),
				md.getVisibility(),
				md.getValue(),
				millisFromEpoch);
	}

	public GeoWaveTimestampMetadata(
			final byte[] primaryId,
			final byte[] secondaryId,
			final byte[] visibility,
			final byte[] value,
			final long millisFromEpoch ) {
		super(
				primaryId,
				secondaryId,
				visibility,
				value);
		this.millisFromEpoch = millisFromEpoch;
	}

	public long getMillisFromEpoch() {
		return millisFromEpoch;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = (prime * result) + (int) (millisFromEpoch ^ (millisFromEpoch >>> 32));
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
		final GeoWaveTimestampMetadata other = (GeoWaveTimestampMetadata) obj;
		if (millisFromEpoch != other.millisFromEpoch) {
			return false;
		}
		return true;
	}
}
