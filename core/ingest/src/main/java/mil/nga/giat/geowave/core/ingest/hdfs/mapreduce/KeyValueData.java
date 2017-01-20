package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The Key-Value pair that will be emitted from a mapper and used by a reducer
 * in the IngestWithReducer flow.
 * 
 * @param <K>
 *            The type for the key to be emitted
 * @param <V>
 *            The type for the value to be emitted
 */
public class KeyValueData<K extends WritableComparable<?>, V extends Writable>
{
	private final K key;
	private final V value;

	public KeyValueData(
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
