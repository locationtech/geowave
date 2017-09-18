package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class MainParamRestFieldValue implements
		RestFieldValue<String>
{
	private final int ordinal;
	private final int totalMainParams;
	private final Field listMainParamField;
	private final Object instance;
	private final RestField<String> delegateField;

	public MainParamRestFieldValue(
			final int ordinal,
			final int totalMainParams,
			final Field listMainParamField,
			final RestField<String> delegateField,
			final Object instance ) {
		this.ordinal = ordinal;
		this.totalMainParams = totalMainParams;
		this.listMainParamField = listMainParamField;
		this.delegateField = delegateField;
		this.instance = instance;
	}

	@Override
	public String getName() {
		return delegateField.getName();
	}

	@Override
	public Class<String> getType() {
		return delegateField.getType();
	}

	@Override
	public String getDescription() {
		return delegateField.getDescription();
	}

	@Override
	public boolean isRequired() {
		return delegateField.isRequired();
	}

	@Override
	public void setValue(
			final String value )
			throws IllegalArgumentException,
			IllegalAccessException {
		listMainParamField.setAccessible(true);
		List<String> currentValue = (List<String>) listMainParamField.get(instance);
		if (currentValue == null) {
			currentValue = new ArrayList<>(
					totalMainParams);
			for (int i = 0; i < totalMainParams; i++) {
				currentValue.add("");
			}
			listMainParamField.set(
					instance,
					currentValue);
		}

		currentValue.set(
				ordinal,
				value);
	}

}
