package mil.nga.giat.geowave.types.gpx;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

public class GpxIngestType implements
		IngestTypePluginProviderSpi<GpxTrack, SimpleFeature>
{
	private static GpxIngestPlugin singletonInstance;

	private static synchronized GpxIngestPlugin getSingletonInstance() {
		if (singletonInstance == null) {
			singletonInstance = new GpxIngestPlugin();
		}
		return singletonInstance;
	}

	@Override
	public StageToHdfsPlugin<GpxTrack> getStageToHdfsPlugin() {
		return getSingletonInstance();
	}

	@Override
	public IngestFromHdfsPlugin<GpxTrack, SimpleFeature> getIngestFromHdfsPlugin() {
		return getSingletonInstance();
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return getSingletonInstance();
	}

	@Override
	public String getIngestTypeName() {
		return "gpx";
	}

	@Override
	public String getIngestTypeDescription() {
		return "xml files adhering to the schema of gps exchange format";
	}

}
