package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.ingest.GeoWaveHBaseData;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.KeyValueData;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author viggy Functionality similar to <code> IngestWithReducer </code>
 */
public interface IngestWithHBaseReducer<I, K extends WritableComparable<?>, V extends Writable, O> extends
		DataAdapterProvider<O>,
		Persistable
{
	public CloseableIterator<KeyValueData<K, V>> toIntermediateMapReduceData(
			I input );

	public CloseableIterator<GeoWaveHBaseData<O>> toGeoWaveData(
			K key,
			ByteArrayId primaryIndexId,
			String globalVisibility,
			Iterable<V> values );
}
