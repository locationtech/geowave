package mil.nga.giat.geowave.store.query;

import java.util.Date;

import mil.nga.giat.geowave.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.GeometryUtils;

import com.vividsolutions.jts.geom.Geometry;

/**
 * The Spatial Temporal Query class represents a query in three dimensions. The
 * constraint that is applied represents an intersection operation on the query
 * geometry AND a date range intersection based on startTime and endTime.
 *
 *
 */
public class SpatialTemporalQuery extends
		SpatialQuery
{
	protected SpatialTemporalQuery() {}

	public SpatialTemporalQuery(
			final Date startTime,
			final Date endTime,
			final Geometry queryGeometry ) {
		super(
				createSpatialTemporalConstraints(
						startTime,
						endTime,
						queryGeometry),
				queryGeometry);
	}

	public SpatialTemporalQuery(
			final TemporalConstraints constraints,
			final Geometry queryGeometry ) {
		super(
				createSpatialTemporalConstraints(
						constraints,
						queryGeometry),
				queryGeometry);
	}

	private static Constraints createSpatialTemporalConstraints(
			final TemporalConstraints temporalConstraints,
			final Geometry queryGeometry ) {
		final Constraints constraints = GeometryUtils.basicConstraintsFromGeometry(queryGeometry);
		for (final TemporalRange range : temporalConstraints.constraints) {
			constraints.constraintsPerTypeOfDimensionDefinition.put(
					TimeDefinition.class,
					new NumericRange(
							range.getStartTime().getTime(),
							range.getEndTime().getTime()));
		}

		return constraints;
	}

	private static Constraints createSpatialTemporalConstraints(
			final Date startTime,
			final Date endTime,
			final Geometry queryGeometry ) {
		final Constraints constraints = GeometryUtils.basicConstraintsFromGeometry(queryGeometry);
		constraints.constraintsPerTypeOfDimensionDefinition.put(
				TimeDefinition.class,
				new NumericRange(
						startTime.getTime(),
						endTime.getTime()));
		return constraints;
	}

}
