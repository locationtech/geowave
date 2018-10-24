package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.query.BaseQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

/**
 * This represent all the constraints and options available in a geowave query.
 * Use QueryBuilder or one of its extensions to construct this object.
 *
 * @param <T>
 *            the type of data being retrieved
 */
public class Query<T> extends
		BaseQuery<T, FilterByTypeQueryOptions<T>>
{

	public Query() {
		super();
	}

	/**
	 * This is better built through QueryBuilder or one of its extensions.
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
