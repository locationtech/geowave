package mil.nga.giat.geowave.core.store.dimension;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class ArrayAdapter<T> implements
		FieldReader<ArrayWrapper<T>>,
		FieldWriter<Object, ArrayWrapper<T>>
{
	protected FieldReader<T[]> reader;
	protected FieldWriter<Object, T[]> writer;

	protected ArrayAdapter() {
		super();
	}

	public ArrayAdapter(
			final ArrayReader<T> reader,
			final ArrayWriter<Object, T> writer ) {
		this.reader = reader;
		this.writer = writer;
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
