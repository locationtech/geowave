package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.ingest.GeoWaveData;

import org.apache.hadoop.io.Writable;

public interface IngestWithReducer<I, K extends Writable, V extends Writable, O> extends
		Persistable
{
	public Iterable<IntermediateData<K, V>> toIntermediateMapReduceData(
			I input );

	public Iterable<GeoWaveData<O>> toGeoWaveData(
			K key,
			Iterable<V> values );
}
