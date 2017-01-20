package mil.nga.giat.geowave.core.geotime.store.field;

import java.nio.ByteBuffer;
import java.util.Date;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class DateSerializationProvider implements
		FieldSerializationProviderSpi<Date>
{

	@Override
	public FieldReader<Date> getFieldReader() {
		return new DateReader();
	}

	@Override
	public FieldWriter<Object, Date> getFieldWriter() {
		return new DateWriter();
	}

	protected static class DateReader implements
			FieldReader<Date>
	{
		@Override
		public Date readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			return new Date(
					ByteBuffer.wrap(
							fieldData).getLong());
		}
	}

	protected static class DateWriter implements
			FieldWriter<Object, Date>
	{
		@Override
		public byte[] writeField(
				final Date fieldData ) {
			if (fieldData == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putLong(fieldData.getTime());
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Date fieldValue ) {
			return new byte[] {};
		}
	}

}
