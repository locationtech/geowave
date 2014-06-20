package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import org.apache.hadoop.io.Writable;

public class IntermediateData<K extends Writable, V extends Writable>
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
