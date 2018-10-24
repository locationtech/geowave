package org.locationtech.geowave.core.store.api;

import java.util.Set;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsQueryBuilderImpl;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;

/**
 * The easiest method to construct an instance of StatisticsQuery.
 *
 * @param <R>
 *            the type for the statistics result
 * @param <B>
 *            the type for the builder, useful for maintaining type with
 *            extensions of this
 */
public interface StatisticsQueryBuilder<R, B extends StatisticsQueryBuilder<R, B>>
{
	/**
	 * set the data type name to filter by when querying for statistics
	 *
	 * @param dataTypeName
	 *            the data type name
	 * @return this builder
	 */
	B dataType(
			String dataTypeName );

	/**
	 *
	 * Add authorization to this builder
	 *
	 * @param authorization
	 *            the authorization
	 * @return this builder
	 */
	B addAuthorization(
			String authorization );

	/**
	 * set the authorizations for this query (it is intersecting with row
	 * visibilities to determine access)
	 *
	 * @param authorizations
	 *            the authorizations
	 * @return this builder
	 */
	B setAuthorizations(
			String[] authorizations );

	/**
	 * set to no authorizations (default behavior)
	 *
	 * @return this builder
	 */
	B noAuthorizations();

	/**
	 * build a statistics query that represents this builder
	 *
	 * @return a statistics query
	 */
	StatisticsQuery<R> build();

	static <R> StatisticsQueryBuilder<R, ?> newBuilder() {
		return new StatisticsQueryBuilderImpl<>();
	}

	QueryByStatisticsTypeFactory factory();

	interface QueryByStatisticsTypeFactory
	{
		BaseStatisticsQueryBuilder<Long> count();

		PartitionStatisticsQueryBuilder<NumericHistogram> rowHistogram();

		IndexStatisticsQueryBuilder<Set<ByteArray>> partitions();
	}
}
