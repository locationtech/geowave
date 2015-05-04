package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

/**
 * This field can be used as a EPSG:4326 latitude dimension within GeoWave. It
 * can utilize JTS geometry as the underlying spatial object for this dimension.
 * 
 */
public class LatitudeField extends
		SpatialField
{
	public LatitudeField() {
		this(
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	public LatitudeField(
			final ByteArrayId fieldId ) {
		this(
				new LatitudeDefinition(),
				fieldId);
	}

	public LatitudeField(
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId ) {
		super(
				baseDefinition,
				fieldId);
	}

	@Override
	public NumericData getNumericData(
			final GeometryWrapper geometry ) {
		return GeometryUtils.latitudeRangeFromGeometry(geometry.getGeometry());
	}

}
