package mil.nga.giat.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class FloatSerializationProvider implements
		FieldSerializationProviderSpi<Float>
{
	@Override
	public FieldReader<Float> getFieldReader() {
		return new FloatReader();
	}

	@Override
	public FieldWriter<Object, Float> getFieldWriter() {
		return new FloatWriter();
	}

	protected static class FloatReader implements
			FieldReader<Float>
	{
		@Override
		public Float readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getFloat();
		}
	}

	protected static class FloatWriter implements
			FieldWriter<Object, Float>
	{
		@Override
		public byte[] writeField(
				final Float fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putFloat(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Float fieldValue ) {
			return new byte[] {};
		}
	}

}
