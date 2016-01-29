package mil.nga.giat.geowave.core.store.query;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class EverythingQuery implements
		Query
{

	public EverythingQuery() {}

	@Override
	public List<QueryFilter> createFilters(
			CommonIndexModel indexModel ) {
		return Collections.<QueryFilter> emptyList();
	}

	@Override
	public boolean isSupported(
			Index index ) {
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		return Collections.emptyList();
	}

}
