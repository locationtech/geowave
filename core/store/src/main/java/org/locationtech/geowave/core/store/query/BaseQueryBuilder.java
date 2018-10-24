package org.locationtech.geowave.core.store.query;

import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactoryImpl;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions.HintKey;

/**
 * A base class for building queries
 *
 * @param <T>
 *            the type of the entries
 * @param <Q>
 *            the type of query (AggregationQuery or Query)
 * @param <R>
 *            the type of the builder, useful for extensions of this to maintain
 *            type
 */
public interface BaseQueryBuilder<T, Q extends BaseQuery<T, ?>, R extends BaseQueryBuilder<T, Q, R>>
{
	/**
	 * choose the appropriate index from all available indices (the default
	 * behavior)
	 *
	 * @return this builder
	 */
	R allIndicies();

	/**
	 * Query only using the specified index
	 *
	 * @param indexName
	 *            the name of the index
	 * @return this builder
	 */
	R indexName(
			String indexName );

	/**
	 * Add authorization to this builder
	 *
	 * @param authorization
	 *            the authorization
	 * @return this builder
	 */
	R addAuthorization(
			String authorization );

	/**
	 * set the authorizations for this query (it is intersecting with row
	 * visibilities to determine access)
	 *
	 * @param authorizations
	 *            the authorizations
	 * @return this builder
	 */
	R setAuthorizations(
			String[] authorizations );

	/**
	 * set to no authorizations (default behavior)
	 *
	 * @return this builder
	 */
	R noAuthorizations();

	/**
	 * set no limit for the number of entries (default behavior)
	 *
	 * @return this builder
	 */
	R noLimit();

	/**
	 * set the limit for the number of entries
	 *
	 * @param limit
	 *            the limit
	 * @return this builder
	 */
	R limit(
			int limit );

	/**
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	<HintValueType> R addHint(
			HintKey<HintValueType> key,
			HintValueType value );

	/**
	 * clear out any hints (default is no hints)
	 *
	 * @return this builder
	 */
	R noHints();

	/**
	 * USe the specified constraints. Constraints can most easily be define by
	 * using the constraintFactory()
	 *
	 * @param constraints
	 *            the constraints
	 * @return this builder
	 */
	R constraints(
			QueryConstraints constraints );

	/**
	 * This is the easiest approach to defining a set of constraints and can be
	 * used to create the constraints that are provided to the constraints
	 * method
	 *
	 * @return a constraints factory
	 */
	default QueryConstraintsFactory constraintsFactory() {
		return QueryConstraintsFactoryImpl.SINGLETON_INSTANCE;
	}

	/**
	 * Build the query represented by this builder
	 *
	 * @return the query
	 */
	Q build();
}
