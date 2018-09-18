package org.locationtech.geowave.core.geotime.store.query.api;

import java.util.Date;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.threeten.extra.Interval;

import com.vividsolutions.jts.geom.Geometry;

public interface SpatialTemporalConstraintsBuilder
{
	SpatialTemporalConstraintsBuilder noSpatialConstraints();

	SpatialTemporalConstraintsBuilder spatialConstraints(
			Geometry geometry );

	SpatialTemporalConstraintsBuilder spatialConstraintsCrs(
			String crsCode );

	SpatialTemporalConstraintsBuilder spatialConstraintsCompareOperation(
			CompareOperation spatialCompareOp );

	SpatialTemporalConstraintsBuilder noTemporalConstraints();

	SpatialTemporalConstraintsBuilder addTimeRange(
			Date startTime,
			Date endTime );

	SpatialTemporalConstraintsBuilder addTimeRange(
			Interval timeRange );

	SpatialTemporalConstraintsBuilder setTimeRanges(
			Interval[] timeRanges );

	QueryConstraints build();

}
