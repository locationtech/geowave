package org.locationtech.geowave.core.store.adapter.statistics;

public class PartitionStatisticsType<R> extends
		StatisticsType<R, PartitionStatisticsQueryBuilder<R>>
{
	private static final long serialVersionUID = 1L;

	public PartitionStatisticsType() {
		super();
	}

	public PartitionStatisticsType(
			final byte[] id ) {
		super(
				id);
	}

	public PartitionStatisticsType(
			final String id ) {
		super(
				id);
	}

	@Override
	public PartitionStatisticsQueryBuilder<R> newBuilder() {
		return new PartitionStatisticsQueryBuilder<>(
				this);
	}

}
