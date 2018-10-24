package org.locationtech.geowave.core.store.adapter.statistics;

public class IndexStatisticsQueryBuilder<R> extends
		StatisticsQueryBuilderImpl<R, IndexStatisticsQueryBuilder<R>>
{
	private String indexName;

	public IndexStatisticsQueryBuilder(
			final StatisticsType<R, IndexStatisticsQueryBuilder<R>> statsType ) {
		this.statsType = statsType;
	}

	public IndexStatisticsQueryBuilder<R> indexName(
			final String indexName ) {
		this.indexName = indexName;
		return this;
	}

	@Override
	protected String extendedId() {
		return indexName;
	}

}
