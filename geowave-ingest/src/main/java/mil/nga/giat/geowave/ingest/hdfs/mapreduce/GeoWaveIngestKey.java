package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.index.ByteArrayId;

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
}
