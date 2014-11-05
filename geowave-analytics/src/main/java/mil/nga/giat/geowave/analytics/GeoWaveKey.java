package mil.nga.giat.geowave.analytics;

import mil.nga.giat.geowave.index.ByteArrayId;

public class GeoWaveKey
{
	private final ByteArrayId adapterId;
	private final ByteArrayId dataId;

	public GeoWaveKey(
			final ByteArrayId adapterId,
			final ByteArrayId dataId ) {
		this.adapterId = adapterId;
		this.dataId = dataId;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = (prime * result) + ((dataId == null) ? 0 : dataId.hashCode());
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
		final GeoWaveKey other = (GeoWaveKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) {
				return false;
			}
		}
		else if (!adapterId.equals(other.adapterId)) {
			return false;
		}
		if (dataId == null) {
			if (other.dataId != null) {
				return false;
			}
		}
		else if (!dataId.equals(other.dataId)) {
			return false;
		}
		return true;
	}
}
