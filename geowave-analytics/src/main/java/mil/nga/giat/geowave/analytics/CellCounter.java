package mil.nga.giat.geowave.analytics;

public interface CellCounter
{
	public void increment(
			long cellId,
			double weight );
}
