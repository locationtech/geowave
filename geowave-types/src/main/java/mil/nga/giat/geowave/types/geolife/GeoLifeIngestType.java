package mil.nga.giat.geowave.types.geolife;

import mil.nga.giat.geowave.ingest.hdfs.HdfsFile;
import mil.nga.giat.geowave.types.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.types.AbstractSimpleFeatureIngestType;

/**
 * This represents an ingest type plugin provider for GeoLife data. It will
 * support ingesting directly from a local file system or staging data from a
 * local files system and ingesting into GeoWave using a map-reduce job.
 */
public class GeoLifeIngestType extends
		AbstractSimpleFeatureIngestType<HdfsFile>
{

	@Override
	protected AbstractSimpleFeatureIngestPlugin<HdfsFile> newPluginInstance() {
		return new GeoLifeIngestPlugin();
	}

	@Override
	public String getIngestTypeName() {
		return "geolife";
	}

	@Override
	public String getIngestTypeDescription() {
		return "files from Microsoft Research GeoLife trajectory data set";
	}
}
