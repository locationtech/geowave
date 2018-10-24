package org.locationtech.geowave.core.store.adapter.statistics;

public class FieldStatisticsQueryBuilder<R> extends
		StatisticsQueryBuilderImpl<R, FieldStatisticsQueryBuilder<R>>
{
	private String fieldName;

	public FieldStatisticsQueryBuilder(
			StatisticsType<R, FieldStatisticsQueryBuilder<R>> statsType ) {
		this.statsType = statsType;
	}

	public FieldStatisticsQueryBuilder<R> fieldName(
			final String fieldName ) {
		this.fieldName = fieldName;
		return this;
	}

	@Override
	protected String extendedId() {
		return fieldName;
	}
}
