package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntermediateData<K extends WritableComparable<?>, V extends Writable>
{
	private final K key;
	private final V value;

	public IntermediateData(
			final K key,
			final V value ) {
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

}
