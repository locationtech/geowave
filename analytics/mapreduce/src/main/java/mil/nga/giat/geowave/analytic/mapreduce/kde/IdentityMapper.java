package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends
		Mapper<DoubleWritable, LongWritable, DoubleWritable, LongWritable>
{
	@Override
	protected void map(
			final DoubleWritable key,
			final LongWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}
}
