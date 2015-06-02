/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.dedupe;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author viggy Functionality similar to <code> GeoWaveDedupeReducer </code>
 */
public class GeoWaveHBaseDedupeReducer extends
		Reducer<GeoWaveHBaseInputKey, ObjectWritable, GeoWaveHBaseInputKey, ObjectWritable>
{

	@Override
	protected void reduce(
			final GeoWaveHBaseInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveHBaseInputKey, ObjectWritable, GeoWaveHBaseInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<ObjectWritable> objects = values.iterator();
		if (objects.hasNext()) {
			context.write(
					key,
					objects.next());
		}
	}
}
