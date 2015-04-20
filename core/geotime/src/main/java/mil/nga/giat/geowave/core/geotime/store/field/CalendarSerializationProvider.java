package mil.nga.giat.geowave.core.geotime.store.field;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.TimeUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class CalendarSerializationProvider implements
		FieldSerializationProviderSpi<Calendar>
{
	@Override
	public FieldReader<Calendar> getFieldReader() {
		return new CalendarReader();
	}

	@Override
	public FieldWriter<Object, Calendar> getFieldWriter() {
		return new CalendarWriter();
	}

	protected static class CalendarReader implements
			FieldReader<Calendar>
	{
		@Override
		public Calendar readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 8)) {
				return null;
			}
			final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			cal.setTime(new Date(
					ByteBuffer.wrap(
							fieldData).getLong()));
			return cal;
		}
	}

	protected static class CalendarWriter implements
			FieldWriter<Object, Calendar>
	{
		@Override
		public byte[] writeField(
				final Calendar cal ) {
			if (cal == null) {
				return new byte[] {};
			}
			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putLong(TimeUtils.calendarToGMTMillis(cal));
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Calendar fieldValue ) {
			return new byte[] {};
		}
	}

}
