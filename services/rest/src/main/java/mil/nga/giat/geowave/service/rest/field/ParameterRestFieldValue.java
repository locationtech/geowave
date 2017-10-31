package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;

import com.beust.jcommander.Parameter;

public class ParameterRestFieldValue extends
		ParameterRestField implements
		RestFieldValue
{
	private final Object instance;

	public ParameterRestFieldValue(
			final Field field,
			final Parameter parameter,
			final Object instance ) {
		super(
				field,
				parameter);
		this.instance = instance;
	}

	@Override
	public void setValue(
			final Object value )
			throws IllegalArgumentException,
			IllegalAccessException {
		field.setAccessible(true);
		field.set(
				instance,
				value);
	}

}
