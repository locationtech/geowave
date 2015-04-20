package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ComparisonCombiningStatsMapper extends
		Mapper<LongWritable, DoubleWritable, LongWritable, DoubleWritable>
{

	@Override
	protected void map(
			final LongWritable key,
			final DoubleWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		long positiveKey = key.get();
		double adjustedValue = value.get();
		if (positiveKey < 0) {
			positiveKey = -positiveKey - 1;
			adjustedValue *= -1;
		}
		super.map(
				new LongWritable(
						positiveKey),
				new DoubleWritable(
						adjustedValue),
				context);
	}

}
