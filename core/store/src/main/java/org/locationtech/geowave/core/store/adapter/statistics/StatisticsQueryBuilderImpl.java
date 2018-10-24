package org.locationtech.geowave.core.store.adapter.statistics;

import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;

public class StatisticsQueryBuilderImpl<R, B extends StatisticsQueryBuilder<R, B>> implements
		StatisticsQueryBuilder<R, B>
{
	private String dataTypeName;
	protected String[] authorizations = new String[0];
	protected StatisticsType<R, B> statsType = null;

	@Override
	public B dataType(
			final String dataTypeName ) {
		this.dataTypeName = dataTypeName;
		return (B) this;
	}

	@Override
	public B addAuthorization(
			final String authorization ) {
		authorizations = (String[]) ArrayUtils.add(
				authorizations,
				authorization);
		return (B) this;
	}

	@Override
	public B setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
		return (B) this;
	}

	@Override
	public B noAuthorizations() {
		this.authorizations = new String[0];
		return (B) this;
	}

	@Override
	public StatisticsQuery<R> build() {
		return new StatisticsQuery<>(
				dataTypeName,
				statsType,
				extendedId(),
				authorizations);
	}

	@Override
	public QueryByStatisticsTypeFactory factory() {
		return QueryByStatisticsTypeFactoryImpl.SINGLETON;
	}

	protected String extendedId() {
		return null;
	}

	protected static class QueryByStatisticsTypeFactoryImpl implements
			QueryByStatisticsTypeFactory
	{
		private static final QueryByStatisticsTypeFactory SINGLETON = new QueryByStatisticsTypeFactoryImpl();

		@Override
		public BaseStatisticsQueryBuilder<Long> count() {
			return CountDataStatistics.STATS_TYPE.newBuilder();
		}

		@Override
		public PartitionStatisticsQueryBuilder<NumericHistogram> rowHistogram() {
			return RowRangeHistogramStatistics.STATS_TYPE.newBuilder();
		}

		@Override
		public IndexStatisticsQueryBuilder<Set<ByteArray>> partitions() {
			return PartitionStatistics.STATS_TYPE.newBuilder();
		}
	}

}
