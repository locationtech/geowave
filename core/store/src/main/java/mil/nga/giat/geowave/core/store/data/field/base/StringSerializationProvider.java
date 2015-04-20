package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class StringSerializationProvider implements
		FieldSerializationProviderSpi<String>
{

	@Override
	public FieldReader<String> getFieldReader() {
		return new StringReader();
	}

	@Override
	public FieldWriter<Object, String> getFieldWriter() {
		return new StringWriter();
	}

	protected static class StringReader implements
			FieldReader<String>
	{

		@Override
		public String readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return StringUtils.stringFromBinary(fieldData);
		}
	}

	protected static class StringWriter implements
			FieldWriter<Object, String>
	{
		@Override
		public byte[] writeField(
				final String fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return StringUtils.stringToBinary(fieldValue);
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final String fieldValue ) {
			return new byte[] {};
		}
	}
}
