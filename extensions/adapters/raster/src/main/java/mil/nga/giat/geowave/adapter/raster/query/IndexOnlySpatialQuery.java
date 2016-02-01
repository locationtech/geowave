package mil.nga.giat.geowave.adapter.raster.query;

import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;

import com.vividsolutions.jts.geom.Geometry;

public class IndexOnlySpatialQuery extends
		SpatialQuery
{

	protected IndexOnlySpatialQuery(
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
	public DistributableQueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] dimensionFields ) {
		// this will ignore fine grained filters and just use the row ID in the
		// index
		return null;
	}

}
