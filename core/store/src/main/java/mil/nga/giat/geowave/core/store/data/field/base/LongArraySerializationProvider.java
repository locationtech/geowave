package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.LongSerializationProvider.LongReader;
import mil.nga.giat.geowave.core.store.data.field.base.LongSerializationProvider.LongWriter;

public class LongArraySerializationProvider implements
		FieldSerializationProviderSpi<Long[]>
{

	@Override
	public FieldReader<Long[]> getFieldReader() {
		return new LongArrayReader();
	}

	@Override
	public FieldWriter<Object, Long[]> getFieldWriter() {
		return new LongArrayWriter();
	}

	private static class LongArrayReader extends
			FixedSizeObjectArrayReader<Long>
	{
		public LongArrayReader() {
			super(
					new LongReader());
		}
	}

	private static class LongArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Long>
	{
		public LongArrayWriter() {
			super(
					new LongWriter());
		}
	}
}
