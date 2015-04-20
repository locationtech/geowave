package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CellSummationCombiner extends
		Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable>
{

	@Override
	public void reduce(
			final LongWritable key,
			final Iterable<DoubleWritable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		double s = 0.0;

		for (final DoubleWritable value : values) {
			s += value.get();
		}
		context.write(
				key,
				new DoubleWritable(
						s));

	}

}
