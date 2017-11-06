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
		// HP Fortify "Access Control" false positive
		// The need to change the accessibility here is
		// necessary, has been review and judged to be safe
		field.setAccessible(true);
		field.set(
				instance,
				value);
	}

}
