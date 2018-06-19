package mil.nga.giat.geowave.core.store.adapter.statistics;

public interface DataStatisticsSet<T> extends
		DataStatistics<T>
{
	public DataStatistics<T>[] getStatisticsSet();
}
