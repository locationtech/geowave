package mil.nga.giat.geowave.core.store.data.field.base;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader.FixedSizeObjectArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.base.DoubleSerializationProvider.DoubleReader;
import mil.nga.giat.geowave.core.store.data.field.base.DoubleSerializationProvider.DoubleWriter;

public class DoubleArraySerializationProvider implements
		FieldSerializationProviderSpi<Double[]>
{

	@Override
	public FieldReader<Double[]> getFieldReader() {
		return new DoubleArrayReader();
	}

	@Override
	public FieldWriter<Object, Double[]> getFieldWriter() {
		return new DoubleArrayWriter();
	}

	private static class DoubleArrayReader extends
			FixedSizeObjectArrayReader<Double>
	{
		public DoubleArrayReader() {
			super(
					new DoubleReader());
		}
	}

	private static class DoubleArrayWriter extends
			FixedSizeObjectArrayWriter<Object, Double>
	{
		public DoubleArrayWriter() {
			super(
					new DoubleWriter());
		}
	}

}
