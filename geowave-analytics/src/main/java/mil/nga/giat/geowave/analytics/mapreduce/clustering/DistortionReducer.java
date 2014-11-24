package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Single reducer gets all point expectations, calculate distortion, and writes to accumulo
 */
public class DistortionReducer extends
		Reducer<IntWritable, DoubleWritable, Text, Mutation>
{
	@Override
	public void reduce(
			final IntWritable key,
			final Iterable<DoubleWritable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		final int numDimensions = Integer.parseInt(context.getConfiguration().get(
				"numDimensions"));
		final String clusterCount = context.getConfiguration().get(
				"cluster.count");
		final String outputRowId = context.getConfiguration().get(
				"jumpRowId");

		double expectation = 0.0;
		int ptCount = 0;
		for (final DoubleWritable value : values) {
			expectation += value.get();
			ptCount++;
		}

		if (ptCount > 0) {
			expectation /= ptCount;

			final Double distortion = Math.pow(
					expectation / numDimensions,
					-(numDimensions / 2));

			// key: jump row id | "DISTORTION" | kk
			// value: distortion value
			final Mutation m = new Mutation(
					outputRowId);
			m.put(
					new Text(
							"DISTORTION"),
					new Text(
							clusterCount),
					new Value(
							distortion.toString().getBytes()));

			// write distortion to accumulo, defaults to table given to
			// AccumuloOutputFormat, in driver
			context.write(
					null,
					m);
		}
	}
}
