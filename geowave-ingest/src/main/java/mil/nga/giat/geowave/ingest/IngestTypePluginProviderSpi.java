package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;

public interface IngestTypePluginProviderSpi<I, O>
{
	public StageToHdfsPlugin<I> getStageToHdfsPlugin()
			throws UnsupportedOperationException;

	public IngestFromHdfsPlugin<I, O> getIngestFromHdfsPlugin()
			throws UnsupportedOperationException;

	public LocalFileIngestPlugin<O> getLocalFileIngestPlugin()
			throws UnsupportedOperationException;

	public String getIngestTypeName();

	public String getIngestTypeDescription();
}
