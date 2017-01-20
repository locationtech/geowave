package mil.nga.giat.geowave.core.geotime.store.field;

import java.util.Date;

import mil.nga.giat.geowave.core.geotime.store.field.DateSerializationProvider.DateReader;
import mil.nga.giat.geowave.core.geotime.store.field.DateSerializationProvider.DateWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;

public class DateArraySerializationProvider implements
		FieldSerializationProviderSpi<Date[]>
{
	@Override
	public FieldReader<Date[]> getFieldReader() {
		return new DateArrayReader();
	}

	@Override
	public FieldWriter<Object, Date[]> getFieldWriter() {
		return new DateArrayWriter();
	}

	private static class DateArrayReader extends
			FixedSizeObjectArrayReader<Date>
	{
		public DateArrayReader() {
			super(
					new DateReader());
		}
	}

	private static class DateArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Date>
	{
		public DateArrayWriter() {
			super(
					new DateWriter());
		}
	}
}
