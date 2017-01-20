package mil.nga.giat.geowave.core.geotime.store.query;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class IndexOnlySpatialQuery extends
		SpatialQuery
{
	protected IndexOnlySpatialQuery() {
		super();
	}

	public IndexOnlySpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry ) {
		super(
				constraints,
				queryGeometry);
	}

	public IndexOnlySpatialQuery(
			final Geometry queryGeometry ) {
		super(
				queryGeometry);
	}

	@Override
	protected DistributableQueryFilter createQueryFilter(
			MultiDimensionalNumericData constraints,
			NumericDimensionField<?>[] orderedConstrainedDimensionFields,
			NumericDimensionField<?>[] unconstrainedDimensionDefinitions ) {
		// this will ignore fine grained filters and just use the row ID in the
		// index
		return null;
	}

}
