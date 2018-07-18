package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;

public interface RestFieldValue<T> extends
		RestField<T>
{
	public void setValue(
			T value )
			throws IllegalArgumentException,
			IllegalAccessException;
	
	public Field getField();
}
