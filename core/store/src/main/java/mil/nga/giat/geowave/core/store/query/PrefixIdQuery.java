package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.PrefixIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class PrefixIdQuery implements
		Query
{
	private ByteArrayId rowPrefix;

	public PrefixIdQuery(
			ByteArrayId rowPrefix ) {
		this.rowPrefix = rowPrefix;
	}

	public ByteArrayId getRowPrefix() {
		return rowPrefix;
	}

	@Override
	public List<QueryFilter> createFilters(
			CommonIndexModel indexModel ) {
		List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.add(new PrefixIdQueryFilter(
				rowPrefix));
		return filters;
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
