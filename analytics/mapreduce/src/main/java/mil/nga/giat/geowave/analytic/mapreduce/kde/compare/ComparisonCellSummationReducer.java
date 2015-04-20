package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import mil.nga.giat.geowave.analytic.mapreduce.kde.CellSummationReducer;

import org.apache.hadoop.io.LongWritable;

public class ComparisonCellSummationReducer extends
		CellSummationReducer
{

	@Override
	protected void collectStats(
			final LongWritable key,
			final double sum,
			final org.apache.hadoop.mapreduce.Reducer.Context context ) {
		long positiveKey = key.get();
		boolean isWinter = false;
		if (positiveKey < 0) {
			positiveKey = -positiveKey - 1;
			isWinter = true;
		}

		final long level = (positiveKey % numLevels) + minLevel;

		context.getCounter(
				"Entries per level (" + (isWinter ? "winter" : "summer") + ")",
				"level " + Long.toString(level)).increment(
				1);
	}
}
