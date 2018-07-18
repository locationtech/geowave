package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;

public class StringMainParam extends
		AbstractMainParam<String>
{

	public StringMainParam(
			int ordinal,
			int totalMainParams,
			Field listMainParamField,
			RestField<String> delegateField,
			Object instance ) {
		super(
				ordinal,
				totalMainParams,
				listMainParamField,
				delegateField,
				instance);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String valueToString(
			String value ) {
		return value;
	}

	public Field getField() {
		return super.getField();
	}
}
