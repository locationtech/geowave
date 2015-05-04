package mil.nga.giat.geowave.analytic.mapreduce.kde;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

abstract public class LevelPartitioner<K> extends
		Partitioner<K, LongWritable>
{
	@Override
	public int getPartition(
			final K key,
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
