package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.AdapterIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class AdapterIdQuery implements
		Query
{
	private ByteArrayId adapterId;

	public AdapterIdQuery(
			ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	@Override
	public List<QueryFilter> createFilters(
			CommonIndexModel indexModel ) {
		List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.add(new AdapterIdQueryFilter(
				adapterId));
		return filters;
	}

	@Override
	public boolean isSupported(
			Index index ) {
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			NumericIndexStrategy indexStrategy ) {
		return Collections.emptyList();
	}

}
