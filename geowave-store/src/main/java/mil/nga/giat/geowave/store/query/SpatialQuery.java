package mil.nga.giat.geowave.store.query;

import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.filter.SpatialQueryFilter;
import com.vividsolutions.jts.geom.Geometry;

/**
 * The Spatial Query class represents a query in two dimensions. The constraint
 * that is applied represents an intersection operation on the query geometry.
 * 
 */
public class SpatialQuery extends
		BasicQuery
{
	private final Geometry queryGeometry;

	/**
	 * Convenience constructor used to construct a SpatialQuery object that has
	 * an X and Y dimension (axis).
	 * 
	 * @param queryGeometry
	 *            spatial geometry of the query
	 */
	public SpatialQuery(
			final Geometry queryGeometry ) {
		super(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry));
		this.queryGeometry = queryGeometry;
	}

	protected SpatialQuery(
			Constraints constraints,
			final Geometry queryGeometry ) {
		super(
				constraints);
		this.queryGeometry = queryGeometry;
	}

	/**
	 * 
	 * @return queryGeometry the spatial geometry of the SpatialQuery object
	 */
	public Geometry getQueryGeometry() {
		return queryGeometry;
	}

	@Override
	protected QueryFilter createQueryFilter(
			MultiDimensionalNumericData constraints,
			DimensionField<?>[] dimensionFields ) {
		return new SpatialQueryFilter(
				constraints,
				dimensionFields,
				queryGeometry);
	}

}
