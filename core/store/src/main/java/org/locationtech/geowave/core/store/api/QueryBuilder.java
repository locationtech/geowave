package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.query.BaseQueryBuilder;
import org.locationtech.geowave.core.store.query.QueryBuilderImpl;

/**
 * A QueryBuilder can be used to easily construct a query which can be used to
 * retrieve data from a GeoWave datastore.
 *
 * @param <T>
 *            the data type
 * @param <R>
 *            the type of the builder so that extensions of this builder can
 *            maintain type
 */
public interface QueryBuilder<T, R extends QueryBuilder<T, R>> extends
		BaseQueryBuilder<T, Query<T>, R>
{
	/**
	 * retrieve all data types (this is the default behavior)
	 *
	 * @return this builder
	 */
	R allTypes();

	/**
	 * add a type name to filter by
	 *
	 * @param typeName
	 *            the type name
	 * @return this builder
	 */
	R addTypeName(
			String typeName );

	/**
	 * set the type names to filter by - an empty array will filter by all
	 * types.
	 *
	 * @param typeNames
	 *            the type names
	 * @return this builder
	 */
	R setTypeNames(
			String[] typeNames );

	/**
	 * Subset fields by field names. If empty it will get all fields.
	 *
	 * @param typeName
	 *            the type name
	 * @param fieldNames
	 *            the field names to subset
	 * @return the entry
	 */
	R subsetFields(
			String typeName,
			String... fieldNames );

	/**
	 * retrieve all fields (this is the default behavior)
	 *
	 * @return this builder
	 */
	R allFields();

	/**
	 * get a default query builder
	 *
	 * @return the new builder
	 */
	static <T> QueryBuilder<T, ?> newBuilder() {
		return new QueryBuilderImpl<>();
	}

}