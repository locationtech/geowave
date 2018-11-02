package org.locationtech.geowave.datastore.redis.util;

import java.util.Arrays;

import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class GeoWaveRedisPersistedRow
{
	private final short numDuplicates;
	private final byte[] dataId;
	private final GeoWaveValue value;

	public GeoWaveRedisPersistedRow(
			final short numDuplicates,
			final byte[] dataId,
			final GeoWaveValue value ) {
		this.numDuplicates = numDuplicates;
		this.dataId = dataId;
		this.value = value;
	}

	public short getNumDuplicates() {
		return numDuplicates;
	}

	public byte[] getDataId() {
		return dataId;
	}

	public byte[] getFieldMask() {
		return value.getFieldMask();
	}

	public byte[] getVisibility() {
		return value.getVisibility();
	}

	public byte[] getValue() {
		return value.getValue();
	}

	public GeoWaveValue getGeoWaveValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(dataId);
		result = (prime * result) + numDuplicates;
		result = (prime * result) + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveRedisPersistedRow other = (GeoWaveRedisPersistedRow) obj;
		if (!Arrays.equals(
				dataId,
				other.dataId)) {
			return false;
		}
		if (numDuplicates != other.numDuplicates) {
			return false;
		}
		if (value == null) {
			if (other.value != null) {
				return false;
			}
		}
		else if (!value.equals(other.value)) {
			return false;
		}
		return true;
	}
}
