package mil.nga.giat.geowave.types.tdrive;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.types.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.types.AbstractSimpleFeatureIngestType;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest type plugin provider for GPX data. It will support
 * ingesting directly from a local file system or staging data from a local
 * files system and ingesting into GeoWave using a map-reduce job.
 */
public class TdriveIngestType extends
		AbstractSimpleFeatureIngestType<TdrivePoint>
{
	protected AbstractSimpleFeatureIngestPlugin<TdrivePoint> newPluginInstance() {
		return new TdriveIngestPlugin();
	}

	@Override
	public String getIngestTypeName() {
		return "tdrive";
	}

	@Override
	public String getIngestTypeDescription() {
		return "files from Microsoft Research T-Drive trajectory data set";
	}

}
