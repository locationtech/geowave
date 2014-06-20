package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public interface IngestPluginBase<I, O>
{

	public WritableDataAdapter<O>[] getDataAdapters(
			String globalVisibility );

	public Iterable<GeoWaveData<O>> toGeoWaveData(
			I input,
			String globalVisibility );
}
