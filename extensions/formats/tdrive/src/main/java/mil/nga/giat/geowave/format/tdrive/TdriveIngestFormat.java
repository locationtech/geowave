package mil.nga.giat.geowave.format.tdrive;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;

/**
 * This represents an ingest format plugin provider for GPX data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class TdriveIngestFormat extends
		AbstractSimpleFeatureIngestFormat<TdrivePoint>
{
	@Override
	protected AbstractSimpleFeatureIngestPlugin<TdrivePoint> newPluginInstance() {
		return new TdriveIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "tdrive";
	}

	@Override
	public String getIngestFormatDescription() {
		return "files from Microsoft Research T-Drive trajectory data set";
	}

}
