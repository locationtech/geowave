package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapContextCellCounter implements
		CellCounter
{
	private final Context context;
	private final long minLevel;
	private final long maxLevel;
	private final long numLevels;
	private final long level;

	public MapContextCellCounter(
			final Context context,
			final long level,
			final long minLevel,
			final long maxLevel ) {
		this.context = context;
		this.level = level;
		this.minLevel = minLevel;
		this.maxLevel = maxLevel;
		numLevels = (maxLevel - minLevel) + 1;
	}

	@Override
	public void increment(
			final long cellId,
			final double weight ) {
		if (weight > 0) {
			try {
				context.write(
						new LongWritable(
								getCellId(cellId)),
						new DoubleWritable(
								weight));
			}
			catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	protected long getCellId(
			final long cellId ) {
		return (cellId * numLevels) + (level - minLevel);
	}

}
