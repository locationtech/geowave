package mil.nga.giat.geowave.types.geolife;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.HdfsFile;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest type plugin provider for GeoLife data. It will support
 * ingesting directly from a local file system or staging data from a local
 * files system and ingesting into GeoWave using a map-reduce job.
 */
public class GeoLifeIngestType implements
		IngestTypePluginProviderSpi<HdfsFile, SimpleFeature>
{
	private static GeoLifeIngestPlugin singletonInstance;

	private static synchronized GeoLifeIngestPlugin getSingletonInstance() {
		if (singletonInstance == null) {
			singletonInstance = new GeoLifeIngestPlugin();
		}
		return singletonInstance;
	}

	@Override
	public StageToHdfsPlugin<HdfsFile> getStageToHdfsPlugin() {
		return getSingletonInstance();
	}

	@Override
	public IngestFromHdfsPlugin<HdfsFile, SimpleFeature> getIngestFromHdfsPlugin() {
		return getSingletonInstance();
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return getSingletonInstance();
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
