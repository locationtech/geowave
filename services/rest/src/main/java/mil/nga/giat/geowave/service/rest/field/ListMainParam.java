package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class ListMainParam extends
		AbstractMainParam<List>
{

	public ListMainParam(
			int ordinal,
			int totalMainParams,
			Field listMainParamField,
			RestField<List> delegateField,
			Object instance ) {
		super(
				ordinal,
				totalMainParams,
				listMainParamField,
				delegateField,
				instance);
	}

	@Override
	protected String valueToString(
			List value ) {
		return StringUtils.join(
				value,
				',');
	}
	
	public Field getField() {
		return super.getField();
	}

}
