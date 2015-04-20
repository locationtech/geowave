package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @param <T>
 *            the native type
 * @param <W>
 *            the writable type
 * 
 */
public interface HadoopWritableSerializer<T, W extends Writable>
{
	public W toWritable(
			T entry );

	public T fromWritable(
			W writable );
}
