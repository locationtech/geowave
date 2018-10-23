package org.locationtech.geowave.datastore.redis.util;

import java.util.Arrays;

import org.locationtech.geowave.core.store.entities.GeoWaveValue;

import com.google.common.primitives.UnsignedBytes;

public class GeoWaveRedisPersistedRow implements
		Comparable<GeoWaveRedisPersistedRow>
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

	@Override
	public int compareTo(
			final GeoWaveRedisPersistedRow obj ) {
		if (this == obj) {
			return 0;
		}
		if (obj == null) {
			return 1;
		}
		if (getClass() != obj.getClass()) {
			return 1;
		}
		final GeoWaveRedisPersistedRow other = obj;
		byte[] otherComp = other.dataId == null ? new byte[0] : other.dataId;
		byte[] thisComp = dataId == null ? new byte[0] : dataId;
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		otherComp = other.getFieldMask() == null ? new byte[0] : other.getFieldMask();
		thisComp = (getFieldMask() == null) && (getFieldMask().length == 0) ? new byte[0] : getFieldMask();
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		otherComp = other.getVisibility() == null ? new byte[0] : other.getVisibility();
		thisComp = getVisibility() == null ? new byte[0] : getVisibility();
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		otherComp = other.getValue() == null ? new byte[0] : other.getValue();
		thisComp = getValue() == null ? new byte[0] : getValue();
		if (UnsignedBytes.lexicographicalComparator().compare(
				thisComp,
				otherComp) != 0) {
			return UnsignedBytes.lexicographicalComparator().compare(
					thisComp,
					otherComp);
		}
		return Integer.compare(
				numDuplicates,
				other.numDuplicates);
	}
}
