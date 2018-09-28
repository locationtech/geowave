package org.locationtech.geowave.core.geotime.store.query;

import java.util.Date;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import com.vividsolutions.jts.geom.Geometry;

public interface SpatialTemporalConstraintBuilder
{
	SpatialTemporalConstraintBuilder spatialConstraint(
			Geometry geometry );

	SpatialTemporalConstraintBuilder spatialConstraintCrs(
			String crsCode );

	SpatialTemporalConstraintBuilder spatialConstraintCompareOperation(
			CompareOperation compareOperation );

	// we can always support open-ended time using beginning of epoch as default
	// start and some end of time such as max long as default end
	SpatialTemporalConstraintBuilder addTimeRange(
			@Nullable Date startTime,
			@Nullable Date endTime );

	SpatialTemporalConstraintBuilder addTimeRange(
			TemporalConstraints timeRange );

	SpatialTemporalConstraintBuilder setTimeRanges(
			TemporalConstraints[] timeRanges );
	
	QueryConstraints build();

}
