package mil.nga.giat.geowave.format.geotools.vector;

import mil.nga.giat.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This represents an ingest format plugin provider for GeoTools vector data
 * stores. It currently only supports ingesting data directly from a local file
 * system into GeoWave.
 */
public class GeoToolsVectorDataStoreIngestFormat implements
		IngestFormatPluginProviderSpi<Object, SimpleFeature>
{
	protected final CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();

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
		return new GeoToolsVectorDataStoreIngestPlugin(
				cqlFilterOptionProvider);
	}

	@Override
	public String getIngestFormatName() {
		return "geotools-vector";
	}

	@Override
	public String getIngestFormatDescription() {
		return "all file-based vector datastores supported within geotools";
	}

	@Override
	public IngestFormatOptionProvider getIngestFormatOptionProvider() {
		return cqlFilterOptionProvider;
	}

}
