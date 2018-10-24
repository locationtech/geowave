package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.BaseQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

/**
 * As the name suggests, an aggregation query is a special-purposed query for
 * performing an aggregation on your dataset. The same set of query criteria can
 * be applied as the input of the aggregation. Typical use should be to use
 *
 * @param <P>
 *            input type for the aggregation
 * @param <R>
 *            result type for the aggregation
 * @param <T>
 *            data type of the entries for the aggregation
 */
public class AggregationQuery<P extends Persistable, R, T> extends
		BaseQuery<R, AggregateTypeQueryOptions<P, R, T>>
{

	/**
	 * default constructor useful only for serialization and deserialization
	 */
	public AggregationQuery() {
		super();
	}

	/**
	 * This constructor should generally not be used directly. Instead use
	 * AggregationQueryBuilder to construct this object.
	 * 
	 * @param commonQueryOptions
	 *            basic query options
	 * @param dataTypeQueryOptions
	 *            query options related to data type
	 * @param indexQueryOptions
	 *            query options related to index
	 * @param queryConstraints
	 *            constraints defining the range of data to query
	 */
	public AggregationQuery(
			final CommonQueryOptions commonQueryOptions,
			final AggregateTypeQueryOptions<P, R, T> dataTypeQueryOptions,
			final IndexQueryOptions indexQueryOptions,
			final QueryConstraints queryConstraints ) {
		super(
				commonQueryOptions,
				dataTypeQueryOptions,
				indexQueryOptions,
				queryConstraints);
	}
}
