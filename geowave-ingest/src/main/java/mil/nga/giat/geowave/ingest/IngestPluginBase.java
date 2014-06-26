package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.index.ByteArrayId;

public interface IngestPluginBase<I, O> extends
		DataAdapterProvider<O>
{
	public Iterable<GeoWaveData<O>> toGeoWaveData(
			I input,
			ByteArrayId primaryIndexId,
			String globalVisibility );
}
