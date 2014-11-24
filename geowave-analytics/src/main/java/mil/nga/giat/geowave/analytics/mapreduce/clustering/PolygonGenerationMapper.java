package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PolygonGenerationMapper extends
		Mapper<Key, Value, IntWritable, Text>
{
	@Override
	/*
	 * Mapper sends pts to respective centroid reducer
	 */
	public void map(
			final Key key,
			final Value value,
			final Context context )
			throws IOException,
			InterruptedException {
		// key: run_uuid_FINAL | centroid# | pt_id
		// value: pt_str

		final Integer centroidId = Integer.parseInt(key.getColumnFamily().toString());
		context.write(
				new IntWritable(
						centroidId),
				new Text(
						value.toString()));
	}
}
