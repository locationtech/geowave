package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.FieldReader;

public class ArrayWrapperReader<T> implements
		FieldReader<ArrayWrapper<T>>
{

	protected FieldReader<T[]> reader;

	protected ArrayWrapperReader() {
		super();
	}

	public ArrayWrapperReader(
			final ArrayReader<T> reader ) {
		this.reader = reader;
	}

	@Override
	public ArrayWrapper<T> readField(
			final byte[] fieldData ) {
		return new ArrayWrapper<>(
				reader.readField(fieldData));
	}
}
