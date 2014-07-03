package mil.nga.giat.geowave.types.geotools;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest type plugin provider for GeoTools data stores. It
 * currently only supports ingesting data directly from a local file system into
 * GeoWave.
 */
public class GeoToolsDataStoreIngestType implements
		IngestTypePluginProviderSpi<Object, SimpleFeature>
{

	@Override
	public StageToHdfsPlugin<Object> getStageToHdfsPlugin() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools files cannot be ingested from HDFS");
	}

	@Override
	public IngestFromHdfsPlugin<Object, SimpleFeature> getIngestFromHdfsPlugin() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools files cannot be ingested from HDFS");
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return new GeoToolsDataStoreIngestPlugin();
	}

	@Override
	public String getIngestTypeName() {
		return "geotools";
	}

	@Override
	public String getIngestTypeDescription() {
		return "all file-based datastores supported within geotools";
	}
}
