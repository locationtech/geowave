package mil.nga.giat.geowave.service.rest.field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RequestParameters
{

	protected Map<String, Object> keyValuePairs;

	protected RequestParameters() {
		keyValuePairs = new HashMap<String, Object>();
	}

	/**
	 * Returns the specified parameter.
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return The value as the specified key name.
	 */
	public Object getValue(
			String parameter ) {
		return keyValuePairs.get(parameter);
	}

	public abstract String getString(
			String parameter );

	public abstract List<?> getList(
			String parameter );

	public abstract Object[] getArray(
			String parameter );

	protected String[] splitStringParameter(
			String parameter ) {
		return getString(
				parameter).split(
				",");
	}
}
