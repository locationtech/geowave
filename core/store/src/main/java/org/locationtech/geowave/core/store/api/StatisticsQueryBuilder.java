package org.locationtech.geowave.core.store.api;

import java.util.Set;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsQueryBuilderImpl;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;

public interface StatisticsQueryBuilder<R, B extends StatisticsQueryBuilder<R, B>>
{
	B dataType(
			String dataTypeName );

	B addAuthorization(
			String authorization );

	B setAuthorizations(
			String[] authorizations );

	B noAuthorizations();

	StatisticsQuery<R> build();

	static <R> StatisticsQueryBuilder<R, ?> newBuilder() {
		return new StatisticsQueryBuilderImpl<>();
	}

	QueryByStatisticsTypeFactory factory();

	interface QueryByStatisticsTypeFactory
	{
		BaseStatisticsQueryBuilder<Long> count();

		PartitionStatisticsQueryBuilder<NumericHistogram> rowHistogram();

		IndexStatisticsQueryBuilder<Set<ByteArrayId>> partitions();
	}
}
