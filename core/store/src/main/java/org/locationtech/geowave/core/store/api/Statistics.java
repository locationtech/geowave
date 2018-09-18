package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;

public interface Statistics<R>
{
	R getResult();

	StatisticsType<R, ?> getType();

	/**
	 * sometimes there are more than one stat per statistical type per data type
	 * and in these cases, this additional identifier is used for uniqueness for
	 * example some statistics are per field (within the data type) or per index
	 * so this identifier would be the field name or index name in those cases
	 *
	 * @return the statistics ID
	 */
	String getId();

	String getDataTypeName();
}
