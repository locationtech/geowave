package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public class DataStatisticsAggregation<T> implements
		Aggregation<DataStatistics<T>, DataStatistics<T>, T>
{
	private DataStatistics<T> statisticsParam;

	private DataStatistics<T> statisticsResult;
	private final byte[] defaultResultBinary;

	public DataStatisticsAggregation(
			final DataStatistics<T> statistics ) {
		this.statisticsResult = statistics;
		this.defaultResultBinary = PersistenceUtils.toBinary(statisticsResult);
		this.statisticsParam = statistics;
	}

	@Override
	public void aggregate(
			final T entry ) {
		statisticsResult.entryIngested(entry);
	}

	@Override
	public DataStatistics<T> getParameters() {
		return statisticsParam;
	}

	@Override
	public void setParameters(
			final DataStatistics<T> parameters ) {
		this.statisticsParam = parameters;
	}

	@Override
	public void clearResult() {
		this.statisticsResult = PersistenceUtils.fromBinary(
				defaultResultBinary,
				DataStatistics.class);
	}

	@Override
	public DataStatistics<T> getResult() {
		return statisticsResult;
	}

}
