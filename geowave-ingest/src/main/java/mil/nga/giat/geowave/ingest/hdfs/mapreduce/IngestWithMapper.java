package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.ingest.IngestPluginBase;

/**
 * This interface is used by the IngestFromHdfsPlugin to implement ingestion
 * within a mapper only. The implementation will be directly persisted to a
 * mapper and called to produce GeoWaveData to be written.
 * 
 * @param <I>
 *            data type for intermediate data
 * @param <O>
 *            data type that will be ingested into GeoWave
 */
public interface IngestWithMapper<I, O> extends
		IngestPluginBase<I, O>,
		Persistable
{

}
