package mil.nga.giat.geowave.core.geotime.store.dimension;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

/**
 * This adapter can be used for reading and writing Geometry fields within
 * GeoWave. The GeometryWrapper wraps JTS geometries with a visibility tag. This
 * enables spatial field definitions that can use JTS geometries.
 * 
 */
public class GeometryAdapter implements
		FieldReader<GeometryWrapper>,
		FieldWriter<Object, GeometryWrapper>
{
	protected final static ByteArrayId DEFAULT_GEOMETRY_FIELD_ID = new ByteArrayId(
			ByteArrayUtils.combineArrays(
					StringUtils.stringToBinary("geom"),
					new byte[] {
						0,
						0
					}));

	public GeometryAdapter() {}

	@Override
	public byte[] writeField(
			final GeometryWrapper geometry ) {
		return GeometryUtils.geometryToBinary(geometry.getGeometry());
	}

	@Override
	public GeometryWrapper readField(
			final byte[] fieldData ) {
		return new GeometryWrapper(
				GeometryUtils.geometryFromBinary(fieldData));
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final ByteArrayId fieldId,
			final GeometryWrapper geometry ) {
		return geometry.getVisibility();
	}
}
