package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalConstraintBuilder;
import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.opengis.filter.Filter;

public interface VectorConstraintsFactory extends QueryConstraintsFactory
{
	SpatialTemporalConstraintBuilder spatialTemporalConstraints();
	
	// these cql expressions should always attempt to use
	// CQLQuery.createOptimalQuery() which requires adapter and index
	QueryConstraints cqlConstraints(
			String cqlExpression );

	QueryConstraints filterConstraints(
			Filter filter );
}
