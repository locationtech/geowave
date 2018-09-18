package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;

public class StatisticsQuery<R>
{
	private final String typeName;
	private final StatisticsType<R, ?> statsType;
	private final String extendedId;
	private final String[] authorizations;

	public StatisticsQuery(
			final String typeName,
			final StatisticsType<R, ?> statsType,
			final String extendedId,
			final String[] authorizations ) {
		super();
		this.typeName = typeName;
		this.statsType = statsType;
		this.extendedId = extendedId;
		this.authorizations = authorizations;
	}

	public String getTypeName() {
		return typeName;
	}

	public StatisticsType<R, ?> getStatsType() {
		return statsType;
	}

	public String getExtendedId() {
		return extendedId;
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

	public StatisticsId getId() {
		return new StatisticsId(
				statsType,
				extendedId);
	}
}
