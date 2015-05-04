package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ComparisonCombinedLevelPartitioner extends
		Partitioner<DoubleWritable, LongWritable>
{
	@Override
	public int getPartition(
			final DoubleWritable key,
			final LongWritable value,
			final int numReduceTasks ) {
		return getPartition(
				value.get(),
				numReduceTasks);
	}

	protected int getPartition(
			final long positiveCellId,
			final int numReduceTasks ) {
		return (int) (positiveCellId % numReduceTasks);
	}
}
