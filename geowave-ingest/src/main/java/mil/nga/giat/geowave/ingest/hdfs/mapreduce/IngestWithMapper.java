package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.ingest.IngestPluginBase;

public interface IngestWithMapper<I, O> extends
		IngestPluginBase<I, O>,
		Persistable
{

}
