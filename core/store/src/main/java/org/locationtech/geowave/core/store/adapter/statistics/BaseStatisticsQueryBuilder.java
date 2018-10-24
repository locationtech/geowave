package org.locationtech.geowave.core.store.adapter.statistics;

public class BaseStatisticsQueryBuilder<R> extends
		StatisticsQueryBuilderImpl<R, BaseStatisticsQueryBuilder<R>>
{

	public BaseStatisticsQueryBuilder(
			final StatisticsType<R, BaseStatisticsQueryBuilder<R>> statsType ) {
		this.statsType = statsType;
	}
}
