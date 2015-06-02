/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import org.apache.hadoop.io.Writable;

/**
 * @author viggy Functionality similar to <code> HBaseHadoopDataAdapter </code>
 */
public interface HBaseHadoopDataAdapter<T, W extends Writable> extends
		DataAdapter<T>
{
	public HBaseHadoopWritableSerializer<T, W> createWritableSerializer();
}
