package mil.nga.giat.geowave.types.geotools.vector;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest type plugin provider for GeoTools vector data
 * stores. It currently only supports ingesting data directly from a local file
 * system into GeoWave.
 */
public class GeoToolsVectorDataStoreIngestType implements
		IngestTypePluginProviderSpi<Object, SimpleFeature>
{

	@Override
	public StageToHdfsPlugin<Object> getStageToHdfsPlugin() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested from HDFS");
	}

	@Override
	public IngestFromHdfsPlugin<Object, SimpleFeature> getIngestFromHdfsPlugin() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GeoTools vector files cannot be ingested from HDFS");
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return new GeoToolsVectorDataStoreIngestPlugin();
	}

	@Override
	public String getIngestTypeName() {
		return "geotools-vector";
	}

	@Override
	public String getIngestTypeDescription() {
		return "all file-based vector datastores supported within geotools";
	}
}
