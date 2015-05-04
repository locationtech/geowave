package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.FloatSerializationProvider.FloatReader;
import mil.nga.giat.geowave.core.store.data.field.base.FloatSerializationProvider.FloatWriter;

public class FloatArraySerializationProvider implements
		FieldSerializationProviderSpi<Float[]>
{

	@Override
	public FieldReader<Float[]> getFieldReader() {
		return new FloatArrayReader();
	}

	@Override
	public FieldWriter<Object, Float[]> getFieldWriter() {
		return new FloatArrayWriter();
	}

	private static class FloatArrayReader extends
			FixedSizeObjectArrayReader<Float>
	{
		public FloatArrayReader() {
			super(
					new FloatReader());
		}
	}

	private static class FloatArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Float>
	{
		public FloatArrayWriter() {
			super(
					new FloatWriter());
		}
	}
}
