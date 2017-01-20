package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import mil.nga.giat.geowave.analytic.mapreduce.kde.MapContextCellCounter;

import org.apache.hadoop.mapreduce.Mapper.Context;

public class NegativeCellIdCounter extends
		MapContextCellCounter
{

	public NegativeCellIdCounter(
			final Context context,
			final long level,
			final long minLevel,
			final long maxLevel ) {
		super(
				context,
				level,
				minLevel,
				maxLevel);
	}

	@Override
	protected long getCellId(
			final long cellId ) {
		return -super.getCellId(cellId) - 1;
	}

}
