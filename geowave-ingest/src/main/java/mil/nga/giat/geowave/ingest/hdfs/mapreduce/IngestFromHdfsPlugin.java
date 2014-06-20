package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.ingest.hdfs.HdfsPluginBase;

public interface IngestFromHdfsPlugin<I, O> extends
		HdfsPluginBase
{
	public boolean isUseReducerPreferred();

	public IngestWithMapper<I, O> ingestWithMapper();

	public IngestWithReducer<I, ?, ?, O> ingestWithReducer();
}
