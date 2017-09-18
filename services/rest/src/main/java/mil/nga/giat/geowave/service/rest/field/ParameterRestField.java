package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;

import com.beust.jcommander.Parameter;

public class ParameterRestField implements
		RestField
{
	protected final Field field;
	protected final Parameter parameter;

	public ParameterRestField(
			final Field field,
			final Parameter parameter ) {
		this.field = field;
		this.parameter = parameter;
	}

	@Override
	public String getName() {
		return field.getName();
	}

	@Override
	public Class<?> getType() {
		return field.getType();
	}

	@Override
	public String getDescription() {
		return parameter.description();
	}

	@Override
	public boolean isRequired() {
		return parameter.required();
	}
}
