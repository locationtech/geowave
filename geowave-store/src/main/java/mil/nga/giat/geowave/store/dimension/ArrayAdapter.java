package mil.nga.giat.geowave.store.dimension;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.field.ArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

public class ArrayAdapter<T extends CommonIndexValue> implements
		FieldReader<ArrayWrapper<T>>,
		FieldWriter<Object, ArrayWrapper<T>>
{
	protected FieldReader<T[]> reader;
	protected FieldWriter<Object, T[]> writer;

	protected ArrayAdapter() {
		super();
	}

	public ArrayAdapter(
			final FieldReader<T> reader,
			final FieldWriter<Object, T> writer ) {
		this.reader = new ArrayReader<T>(
				reader);
		this.writer = new ArrayWriter<Object, T>(
				writer);
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final ByteArrayId fieldId,
			final ArrayWrapper<T> fieldValue ) {
		return fieldValue.getVisibility();
	}

	@Override
	public byte[] writeField(
			final ArrayWrapper<T> fieldValue ) {
		return writer.writeField(fieldValue.getArray());
	}

	@Override
	public ArrayWrapper<T> readField(
			final byte[] fieldData ) {
		return new ArrayWrapper<T>(
				reader.readField(fieldData));
	}

}
