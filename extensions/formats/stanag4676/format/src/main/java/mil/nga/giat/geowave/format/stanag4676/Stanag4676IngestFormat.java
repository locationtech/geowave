package mil.nga.giat.geowave.format.stanag4676;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

public class Stanag4676IngestFormat implements
		IngestFormatPluginProviderSpi<WholeFile, Object>
{
	private static Stanag4676IngestPlugin singletonInstance;

	private static synchronized Stanag4676IngestPlugin getSingletonInstance() {
		if (singletonInstance == null) {
			singletonInstance = new Stanag4676IngestPlugin();
		}
		return singletonInstance;
	}

	@Override
	public AvroFormatPlugin<WholeFile, Object> getAvroFormatPlugin()
			throws UnsupportedOperationException {
		return getSingletonInstance();
	}

	@Override
	public IngestFromHdfsPlugin<WholeFile, Object> getIngestFromHdfsPlugin()
			throws UnsupportedOperationException {
		return getSingletonInstance();
	}

	@Override
	public LocalFileIngestPlugin<Object> getLocalFileIngestPlugin()
			throws UnsupportedOperationException {
		return getSingletonInstance();
	}

	@Override
	public String getIngestFormatName() {
		return "stanag4676";
	}

	@Override
	public String getIngestFormatDescription() {
		return "xml files representing track data that adheres to the schema defined by STANAG-4676";
	}

	@Override
	public IngestFormatOptions getIngestFormatOptionProvider() {
		// for now don't support filtering
		return null;
	}

}
