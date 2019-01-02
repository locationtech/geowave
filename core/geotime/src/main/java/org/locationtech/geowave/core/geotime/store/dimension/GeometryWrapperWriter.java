package org.locationtech.geowave.core.geotime.store.dimension;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class GeometryWrapperWriter implements
		FieldWriter<Object, GeometryWrapper>
{

	private Integer geometryPrecision = null;

	public GeometryWrapperWriter(
			@Nullable Integer geometryPrecision ) {
		this.geometryPrecision = geometryPrecision;
	}

	public void setPrecision(
			@Nullable Integer geometryPrecision ) {
		this.geometryPrecision = geometryPrecision;
	}

	@Override
	public byte[] writeField(
			final GeometryWrapper geometry ) {
		return GeometryUtils.geometryToBinary(
				geometry.getGeometry(),
				geometryPrecision);
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final String fieldName,
			final GeometryWrapper geometry ) {
		return geometry.getVisibility();
	}
}
