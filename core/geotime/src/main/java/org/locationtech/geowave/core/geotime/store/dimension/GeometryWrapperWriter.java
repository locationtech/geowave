package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class GeometryWrapperWriter implements
		FieldWriter<Object, GeometryWrapper>
{

	@Override
	public byte[] writeField(
			final GeometryWrapper geometry ) {
		return GeometryUtils.geometryToBinary(geometry.getGeometry());
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final String fieldName,
			final GeometryWrapper geometry ) {
		return geometry.getVisibility();
	}
}
