package mil.nga.giat.geowave.core.store.query;

import java.util.List;

import mil.nga.giat.geowave.core.store.filter.QueryFilter;

public interface FilteredIndexQuery
{

	public void setClientFilters(
			final List<QueryFilter> clientFilters );

}
