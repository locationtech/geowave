package mil.nga.giat.geowave.analytic.mapreduce.kde;

public interface CellCounter
{
	public void increment(
			long cellId,
			double weight );
}
