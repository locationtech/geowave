package org.locationtech.geowave.core.store.adapter.statistics;

public class FieldStatisticsType<R> extends
		StatisticsType<R, FieldStatisticsQueryBuilder<R>>
{
	private static final long serialVersionUID = 1L;

	public FieldStatisticsType() {
		super();
	}

	public FieldStatisticsType(
			byte[] id ) {
		super(
				id);
	}

	public FieldStatisticsType(
			String id ) {
		super(
				id);
	}

	@Override
	public FieldStatisticsQueryBuilder<R> newBuilder() {
		return new FieldStatisticsQueryBuilder<>(
				this);
	}

}
