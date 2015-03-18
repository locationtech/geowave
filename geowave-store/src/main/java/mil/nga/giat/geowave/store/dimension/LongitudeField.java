package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.store.GeometryUtils;

/**
 * This field can be used as a EPSG:4326 longitude dimension within GeoWave. It
 * can utilize JTS geometry as the underlying spatial object for this dimension.
 * 
 */
public class LongitudeField extends
		SpatialField
{
	public LongitudeField() {
		this(
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID);
	}

	public LongitudeField(
			final ByteArrayId fieldId ) {
		this(
				new LongitudeDefinition(),
				fieldId);
	}

	public LongitudeField(
			final NumericDimensionDefinition baseDefinition,
			final ByteArrayId fieldId ) {
		super(
				baseDefinition,
				fieldId);
	}

	@Override
	public NumericData getNumericData(
			final GeometryWrapper geometry ) {
		return GeometryUtils.longitudeRangeFromGeometry(geometry.getGeometry());
	}

}
