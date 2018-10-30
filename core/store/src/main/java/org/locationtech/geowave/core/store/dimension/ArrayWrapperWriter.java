package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.store.data.field.ArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class ArrayWrapperWriter<T> implements
		FieldWriter<Object, ArrayWrapper<T>>
{
	protected FieldWriter<Object, T[]> writer;

	protected ArrayWrapperWriter() {
		super();
	}

	public ArrayWrapperWriter(
			final ArrayWriter<Object, T> writer ) {
		this.writer = writer;

	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final String fieldName,
			final ArrayWrapper<T> fieldValue ) {
		return fieldValue.getVisibility();
	}

	@Override
	public byte[] writeField(
			final ArrayWrapper<T> fieldValue ) {
		return writer.writeField(fieldValue.getArray());
	}
}
