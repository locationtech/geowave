package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;

public class GeometryWrapperReader implements
		FieldReader<GeometryWrapper>
{

	@Override
	public GeometryWrapper readField(
			final byte[] fieldData ) {
		return new GeometryWrapper(
				GeometryUtils.geometryFromBinary(fieldData));
	}

}
