package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

public class CustomCRSSpatialField extends
		SpatialField
{
	public CustomCRSSpatialField() {}

	public CustomCRSSpatialField(
			final CustomCRSSpatialDimension baseDefinition ) {
		this(
				baseDefinition,
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	public CustomCRSSpatialField(
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId ) {
		super(
				baseDefinition,
				fieldId);
	}

	@Override
	public NumericData getNumericData(
			final GeometryWrapper geometry ) {
		// TODO if this can be generalized to n-dimensional that would be better
		if (((CustomCRSSpatialDimension) baseDefinition).axis == 0) {
			return GeometryUtils.xRangeFromGeometry(geometry.getGeometry());
		}
		return GeometryUtils.yRangeFromGeometry(geometry.getGeometry());
	}
}
