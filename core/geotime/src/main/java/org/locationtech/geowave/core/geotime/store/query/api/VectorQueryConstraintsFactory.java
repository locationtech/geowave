package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.opengis.filter.Filter;

/**
 * A constraints factory for building constraints for SimpleFeature data.
 *
 */
public interface VectorQueryConstraintsFactory extends
		QueryConstraintsFactory
{

	/**
	 * get a builder for spatiotemporal constraints
	 *
	 * @return the builder
	 */
	SpatialTemporalConstraintsBuilder spatialTemporalConstraints();

	/**
	 * create query constraints representing an OGC filter on vector data
	 *
	 * @param filter
	 *            the OGC filter
	 * @return the query constraints
	 */
	QueryConstraints filterConstraints(
			final Filter filter );

	/**
	 * create query constraints representing this CQL expression (see
	 * Geoserver's syntax guide:
	 * https://docs.geoserver.org/latest/en/user/filter/ecql_reference.html)
	 *
	 * @param cqlExpression
	 *            the CQL expression
	 * @return this builder
	 */
	QueryConstraints cqlConstraints(
			final String cqlExpression );
}
