package org.locationtech.geowave.core.geotime.store.statistics;

import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsQueryBuilderImpl;
import org.threeten.extra.Interval;

import com.vividsolutions.jts.geom.Envelope;

public class VectorStatisticsQueryBuilderImpl<R> extends
		StatisticsQueryBuilderImpl<R, VectorStatisticsQueryBuilder<R>> implements
		VectorStatisticsQueryBuilder<R>
{
	@Override
	public QueryByVectorStatisticsTypeFactory factory() {
		return QueryByVectorStatisticsTypeFactoryImpl.SINGLETON;
	}

	protected static class QueryByVectorStatisticsTypeFactoryImpl extends
			QueryByStatisticsTypeFactoryImpl implements
			QueryByVectorStatisticsTypeFactory
	{
		private static QueryByVectorStatisticsTypeFactory SINGLETON = new QueryByVectorStatisticsTypeFactoryImpl();

		@Override
		public FieldStatisticsQueryBuilder<Envelope> bbox() {
			return BoundingBoxDataStatistics.STATS_TYPE.newBuilder();
		}

		@Override
		public FieldStatisticsQueryBuilder<Interval> timeRange() {
			return TimeRangeDataStatistics.STATS_TYPE.newBuilder();
		}
	}
}
