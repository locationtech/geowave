package mil.nga.giat.geowave.datastore.accumulo.mapreduce.dedupe;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A basic implementation of deduplication as a reducer
 */
public class GeoWaveDedupeReducer extends
		Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>
{

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
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
