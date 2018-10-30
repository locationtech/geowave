package org.locationtech.geowave.core.store.adapter.statistics;

public class BaseStatisticsType<R> extends
		StatisticsType<R, BaseStatisticsQueryBuilder<R>>
{
	private static final long serialVersionUID = 1L;

	public BaseStatisticsType() {
		super();
	}

	public BaseStatisticsType(
			final byte[] id ) {
		super(
				id);
	}

	public BaseStatisticsType(
			final String id ) {
		super(
				id);
	}

	@Override
	public BaseStatisticsQueryBuilder<R> newBuilder() {
		return new BaseStatisticsQueryBuilder<>(
				this);
	}

}
