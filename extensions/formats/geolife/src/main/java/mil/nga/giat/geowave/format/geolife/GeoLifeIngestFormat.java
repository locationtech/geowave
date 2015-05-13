package mil.nga.giat.geowave.format.geolife;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;

/**
 * This represents an ingest format plugin provider for GeoLife data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class GeoLifeIngestFormat extends
		AbstractSimpleFeatureIngestFormat<WholeFile>
{

	@Override
	protected AbstractSimpleFeatureIngestPlugin<WholeFile> newPluginInstance() {
		return new GeoLifeIngestPlugin();
	}

	@Override
	public String getIngestFormatName() {
		return "geolife";
	}

	@Override
	public String getIngestFormatDescription() {
		return "files from Microsoft Research GeoLife trajectory data set";
	}
}
