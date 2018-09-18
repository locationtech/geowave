package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.opengis.filter.Filter;

public interface VectorQueryConstraintsFactory extends
		QueryConstraintsFactory
{

	SpatialTemporalConstraintsBuilder spatialTemporalConstraints();

	QueryConstraints filterConstraints(
			final Filter filter );

	QueryConstraints cqlConstraints(
			final String cqlExpression );
}
