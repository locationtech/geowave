package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.index.ByteArrayId;

/**
 * This class encapsulates the unique identifier for GeoWave to ingest data
 * using a map-reduce GeoWave output format. The record writer must have bother
 * the adapter and the index for the data element to ingest.
 */
public class GeoWaveIngestKey
{
	private final ByteArrayId indexId;
	private final ByteArrayId adapterId;

	public GeoWaveIngestKey(
			final ByteArrayId indexId,
			final ByteArrayId adapterId ) {
		this.indexId = indexId;
		this.adapterId = adapterId;
	}

	public ByteArrayId getIndexId() {
		return indexId;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = (prime * result) + ((indexId == null) ? 0 : indexId.hashCode());
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
		final GeoWaveIngestKey other = (GeoWaveIngestKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) {
				return false;
			}
		}
		else if (!adapterId.equals(other.adapterId)) {
			return false;
		}
		if (indexId == null) {
			if (other.indexId != null) {
				return false;
			}
		}
		else if (!indexId.equals(other.indexId)) {
			return false;
		}
		return true;
	}
}
