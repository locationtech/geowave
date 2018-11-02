package org.locationtech.geowave.core.geotime.store.query.api;

import java.util.Date;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.threeten.extra.Interval;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This is a builder for creating purely spatiotemporal query constraints
 */
public interface SpatialTemporalConstraintsBuilder
{
	/**
	 * clear any spatial constraints
	 * 
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder noSpatialConstraints();

	/**
	 * set a geometry as a spatial constraint
	 * 
	 * @param geometry
	 *            the geometry
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder spatialConstraints(
			Geometry geometry );

	/**
	 * set a Coordinate Reference System code to use associated with this
	 * builder's geometry. If no geometry is set, this is inconsequential.
	 * 
	 * @param crsCode
	 *            the CRS code
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder spatialConstraintsCrs(
			String crsCode );

	/**
	 * set a relational operation when comparing geometries to be uses with this
	 * builder's geometry. If no geometry is set, this is inconsequential.
	 * 
	 * @param spatialCompareOp
	 *            the compare operation
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder spatialConstraintsCompareOperation(
			CompareOperation spatialCompareOp );

	/**
	 * clear any temporal constraints
	 * 
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder noTemporalConstraints();

	/**
	 * add a time range
	 * 
	 * @param startTime
	 *            the start of the range
	 * @param endTime
	 *            the end of the range
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder addTimeRange(
			Date startTime,
			Date endTime );

	/**
	 * add a time range as an interval
	 * 
	 * @param timeRange
	 *            the time range
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder addTimeRange(
			Interval timeRange );

	/**
	 * set the time ranges to this array of intervals
	 * 
	 * @param timeRanges
	 *            the time ranges
	 * @return this builder
	 */
	SpatialTemporalConstraintsBuilder setTimeRanges(
			Interval[] timeRanges );

	/**
	 * build a query constraints that represents the spatiotemporal constraints
	 * of this builder
	 * 
	 * @return the constraints
	 */
	QueryConstraints build();

}
