package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.statistics.VectorStatisticsQueryBuilderImpl;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.threeten.extra.Interval;

import com.vividsolutions.jts.geom.Envelope;

/**
 *
 * A StatisticsQueryBuilder for vector (SimpleFeature) data. This should be
 * preferred as the mechanism for constructing a statistics query in all cases
 * when working with SimpleFeature data.
 *
 * @param <R>
 */
public interface VectorStatisticsQueryBuilder<R> extends
		StatisticsQueryBuilder<R, VectorStatisticsQueryBuilder<R>>
{
	/**
	 * create a new builder of this type
	 *
	 * @return a new builder
	 */
	static <R> VectorStatisticsQueryBuilder<R> newBuilder() {
		return new VectorStatisticsQueryBuilderImpl<>();
	}

	@Override
	QueryByVectorStatisticsTypeFactory factory();

	interface QueryByVectorStatisticsTypeFactory extends
			QueryByStatisticsTypeFactory
	{
		/**
		 * get Bounding Box statistics
		 *
		 * @return a statistics query builder for bounding box statistics
		 */
		FieldStatisticsQueryBuilder<Envelope> bbox();

		/**
		 * get time range statistics
		 *
		 * @return a statistics query builder for time range statistics
		 */
		FieldStatisticsQueryBuilder<Interval> timeRange();
	}
}
