package mil.nga.giat.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class IntegerSerializationProvider implements
		FieldSerializationProviderSpi<Integer>
{

	@Override
	public FieldReader<Integer> getFieldReader() {
		return new IntegerReader();
	}

	@Override
	public FieldWriter<Object, Integer> getFieldWriter() {
		return new IntegerWriter();
	}

	protected static class IntegerReader implements
			FieldReader<Integer>
	{
		@Override
		public Integer readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 4)) {
				return null;
			}
			return ByteBuffer.wrap(
					fieldData).getInt();
		}

	}

	protected static class IntegerWriter implements
			FieldWriter<Object, Integer>
	{
		@Override
		public byte[] writeField(
				final Integer fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putInt(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Integer fieldValue ) {
			return new byte[] {};
		}
	}
}
