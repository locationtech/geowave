package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.statistics.VectorStatisticsQueryBuilderImpl;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.threeten.extra.Interval;

import com.vividsolutions.jts.geom.Envelope;

public interface VectorStatisticsQueryBuilder<R> extends
		StatisticsQueryBuilder<R, VectorStatisticsQueryBuilder<R>>
{
	static <R> VectorStatisticsQueryBuilder<R> newBuilder() {
		return new VectorStatisticsQueryBuilderImpl<>();
	}

	@Override
	QueryByVectorStatisticsTypeFactory factory();

	interface QueryByVectorStatisticsTypeFactory extends
			QueryByStatisticsTypeFactory
	{
		FieldStatisticsQueryBuilder<Envelope> bbox();

		FieldStatisticsQueryBuilder<Interval> timeRange();
	}
}
