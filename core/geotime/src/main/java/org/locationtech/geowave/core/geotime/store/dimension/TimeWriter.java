package org.locationtech.geowave.core.geotime.store.dimension;

import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class TimeWriter implements
		FieldWriter<Object, Time>
{

	public TimeWriter() {}

	@Override
	public byte[] writeField(
			final Time time ) {
		return time.toBinary();
	}

	@Override
	public byte[] getVisibility(
			final Object rowValue,
			final String fieldName,
			final Time time ) {
		return time.getVisibility();
	}
}
