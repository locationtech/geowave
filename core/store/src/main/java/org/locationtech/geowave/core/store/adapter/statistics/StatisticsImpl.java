package org.locationtech.geowave.core.store.adapter.statistics;

import org.locationtech.geowave.core.store.api.Statistics;

public class StatisticsImpl<R> implements
		Statistics<R>
{
	private final R result;
	private final StatisticsType<R, ?> statsType;
	private final String statsId;
	private final String dataTypeName;

	public StatisticsImpl(
			final R result,
			final StatisticsType<R, ?> statsType,
			final String statsId,
			final String dataTypeName ) {
		this.result = result;
		this.statsType = statsType;
		this.statsId = statsId;
		this.dataTypeName = dataTypeName;
	}

	@Override
	public R getResult() {
		return result;
	}

	@Override
	public String getId() {
		return statsId;
	}

	@Override
	public String getDataTypeName() {
		return dataTypeName;
	}

	@Override
	public StatisticsType<R, ?> getType() {
		return statsType;
	}

}
