package mil.nga.giat.geowave.analytics.mapreduce.geosearch.clustering;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PolygonGenerationMapper extends Mapper<Key, Value, IntWritable, Text>
{
	@Override
	/*
	 * Mapper sends pts to respective centroid reducer
	 */
	public void  map(Key key, Value value, Context context) throws IOException, InterruptedException {
		System.out.println("PolygonGenerator Mapping...");
		// key: run_uuid_FINAL | centroid# | pt_id
		// value: pt_str

		Integer centroidId = Integer.parseInt(key.getColumnFamily().toString());
		context.write(new IntWritable(centroidId), new Text(value.toString()));
	}
}
