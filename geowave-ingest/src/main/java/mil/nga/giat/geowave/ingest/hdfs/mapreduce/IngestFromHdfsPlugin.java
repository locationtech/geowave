package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.ingest.hdfs.HdfsPluginBase;
import mil.nga.giat.geowave.store.index.Index;

public interface IngestFromHdfsPlugin<I, O> extends
		HdfsPluginBase
{
	public boolean isUseReducerPreferred();

	public IngestWithMapper<I, O> ingestWithMapper();

	public IngestWithReducer<I, ?, ?, O> ingestWithReducer();

	public Index[] getSupportedIndices();
	
	public Index[] getRequiredIndices();

}
