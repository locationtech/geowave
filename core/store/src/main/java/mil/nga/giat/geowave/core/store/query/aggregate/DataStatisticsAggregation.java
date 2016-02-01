package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public class DataStatisticsAggregation<T> implements
		Aggregation<T>
{
	private DataStatistics<T> statistics;

	public DataStatisticsAggregation(
			final DataStatistics<T> statistics ) {
		this.statistics = statistics;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		statistics.merge(merge);
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(statistics);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		statistics = PersistenceUtils.fromBinary(
				bytes,
				DataStatistics.class);
	}

	@Override
	public void aggregate(
			final T entry ) {
		statistics.entryIngested(
				null,
				entry);
	}

}
