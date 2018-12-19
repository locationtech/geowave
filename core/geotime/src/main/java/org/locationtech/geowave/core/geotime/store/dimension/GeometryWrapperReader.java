package org.locationtech.geowave.core.geotime.store.dimension;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;

public class GeometryWrapperReader implements
		FieldReader<GeometryWrapper>
{

	private Integer geometryPrecision = null;

	public GeometryWrapperReader(
			@Nullable Integer geometryPrecision ) {
		this.geometryPrecision = geometryPrecision;
	}

	public void setPrecision(
			@Nullable Integer geometryPrecision ) {
		this.geometryPrecision = geometryPrecision;
	}

	@Override
	public GeometryWrapper readField(
			final byte[] fieldData ) {
		return new GeometryWrapper(
				GeometryUtils.geometryFromBinary(
						fieldData,
						geometryPrecision));
	}
}
