package org.locationtech.geowave.core.store.adapter.statistics;

public class IndexStatisticsType<R> extends
		StatisticsType<R, IndexStatisticsQueryBuilder<R>>
{
	private static final long serialVersionUID = 1L;

	public IndexStatisticsType() {
		super();
	}

	public IndexStatisticsType(
			byte[] id ) {
		super(
				id);
	}

	public IndexStatisticsType(
			String id ) {
		super(
				id);
	}

	@Override
	public IndexStatisticsQueryBuilder<R> newBuilder() {
		return new IndexStatisticsQueryBuilder<>(
				this);
	}

}
