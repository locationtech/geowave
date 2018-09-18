package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.query.BaseQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

public class Query<T> extends
		BaseQuery<T, FilterByTypeQueryOptions<T>>
{

	public Query() {
		super();
	}

	/**
	 * This is better built through QueryBuilder.
	 * 
	 * @param commonQueryOptions
	 * @param dataTypeQueryOptions
	 * @param indexQueryOptions
	 * @param queryConstraints
	 */
	public Query(
			final CommonQueryOptions commonQueryOptions,
			final FilterByTypeQueryOptions<T> dataTypeQueryOptions,
			final IndexQueryOptions indexQueryOptions,
			final QueryConstraints queryConstraints ) {
		super(
				commonQueryOptions,
				dataTypeQueryOptions,
				indexQueryOptions,
				queryConstraints);
	}
}
