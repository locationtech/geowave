package org.locationtech.geowave.core.store.query.constraints;

import java.util.List;

import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public interface AdapterAndIndexBasedQueryConstraints extends QueryConstraints
{
	QueryConstraints createQueryConstraints(
			DataTypeAdapter<?> adapter,
			Index index );

	@Override
	default List<QueryFilter> createFilters(
			Index index ) {
		return null;
	}

	@Override
	default List<MultiDimensionalNumericData> getIndexConstraints(
			Index index ) {
		return null;
	}
}
