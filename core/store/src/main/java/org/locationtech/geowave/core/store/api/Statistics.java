package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;

public interface Statistics<R>
{
	R getResult();

	StatisticsType<R> getType();

	String getDataTypeId();
}
