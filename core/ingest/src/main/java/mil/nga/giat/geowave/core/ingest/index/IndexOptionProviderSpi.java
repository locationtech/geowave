package mil.nga.giat.geowave.core.ingest.index;

import mil.nga.giat.geowave.core.ingest.GeoWaveCLIOptionsProvider;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexOptionProviderSpi extends
		GeoWaveCLIOptionsProvider
{
	public PrimaryIndex wrapIndexWithOptions(
			PrimaryIndex index );

	public int getResolutionOrder();
}
