package mil.nga.giat.geowave.core.geotime.store.field;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

import com.vividsolutions.jts.geom.Geometry;

public class GeometrySerializationProvider implements
		FieldSerializationProviderSpi<Geometry>
{
	@Override
	public FieldReader<Geometry> getFieldReader() {
		return new GeometryReader();
	}

	@Override
	public FieldWriter<Object, Geometry> getFieldWriter() {
		return new GeometryWriter();
	}

	protected static class GeometryReader implements
			FieldReader<Geometry>
	{
		@Override
		public Geometry readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return GeometryUtils.geometryFromBinary(fieldData);
		}
	}

	protected static class GeometryWriter implements
			FieldWriter<Object, Geometry>
	{
		@Override
		public byte[] writeField(
				final Geometry fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return GeometryUtils.geometryToBinary(fieldValue);
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Geometry fieldValue ) {
			return new byte[] {};
		}
	}

}
