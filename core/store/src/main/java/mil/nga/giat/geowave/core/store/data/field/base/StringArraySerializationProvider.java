package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.VariableSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.StringSerializationProvider.StringReader;
import mil.nga.giat.geowave.core.store.data.field.base.StringSerializationProvider.StringWriter;

public class StringArraySerializationProvider implements
		FieldSerializationProviderSpi<String[]>
{

	@Override
	public FieldReader<String[]> getFieldReader() {
		return new StringArrayReader();
	}

	@Override
	public FieldWriter<Object, String[]> getFieldWriter() {
		return new StringArrayWriter();
	}

	private static class StringArrayReader extends
			VariableSizeObjectArrayReader<String>
	{
		public StringArrayReader() {
			super(
					new StringReader());
		}
	}

	private static class StringArrayWriter extends
			VariableSizeObjectArrayWriter<Object, String>
	{
		public StringArrayWriter() {
			super(
					new StringWriter());
		}
	}
}
