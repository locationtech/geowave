package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.IntegerSerializationProvider.IntegerReader;
import mil.nga.giat.geowave.core.store.data.field.base.IntegerSerializationProvider.IntegerWriter;

public class IntegerArraySerializationProvider implements
		FieldSerializationProviderSpi<Integer[]>
{

	@Override
	public FieldReader<Integer[]> getFieldReader() {
		return new IntegerArrayReader();
	}

	@Override
	public FieldWriter<Object, Integer[]> getFieldWriter() {
		return new IntegerArrayWriter();
	}

	private static class IntegerArrayReader extends
			FixedSizeObjectArrayReader<Integer>
	{
		public IntegerArrayReader() {
			super(
					new IntegerReader());
		}
	}

	private static class IntegerArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Integer>
	{
		public IntegerArrayWriter() {
			super(
					new IntegerWriter());
		}
	}

}
