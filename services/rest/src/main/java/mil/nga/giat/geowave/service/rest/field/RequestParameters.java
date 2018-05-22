package mil.nga.giat.geowave.service.rest.field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Form;
import org.restlet.representation.Representation;

public class RequestParameters
{

	private Map<String, Object> keyValuePairs;

	public RequestParameters(
			Representation request )
			throws IOException {
		keyValuePairs = new HashMap<String, Object>();
		injectJsonParams(request.getText());
	}

	public RequestParameters(
			Form form ) {
		keyValuePairs = new HashMap<String, Object>();
		injectFormParams(form);
	}

	/**
	 * Returns the specified parameter.
	 * 
	 * Possible return types: String List<String>
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return The value as the specified key name.
	 */
	public Object getValue(
			String parameter ) {
		return keyValuePairs.get(parameter);
	}

	public String getString(
			String parameter ) {
		return (String) getValue(parameter);
	}

	public List<?> getList(
			String parameter ) {
		Object value = getValue(parameter);
		try {
			return (List<?>) value;
		}
		catch (ClassCastException e) {
			try {
				return Arrays.asList(((String) value).split(","));
			}
			catch (ClassCastException ee) {
				return jsonArrayToList((JSONArray) value);
			}

		}
	}

	public Object[] getArray(
			String parameter ) {
		Object value = getValue(parameter);
		try {
			return (Object[]) value;
		}
		catch (ClassCastException e) {
			try {
				return ((String) value).split(",");
			}
			catch (ClassCastException ee) {
				return jsonArrayToArray((JSONArray) value);
			}

		}
	}

	private void injectFormParams(
			Form form ) {
		for (String key : form.getNames()) {
			keyValuePairs.put(
					key,
					form.getFirst(
							key).getValue());
		}
	}

	private void injectJsonParams(
			String jsonString ) {
		JSONObject json = new JSONObject(
				jsonString);
		for (String key : json.keySet()) {
			try {
				keyValuePairs.put(
						key,
						json.getJSONArray(key));
				continue;
			}
			catch (JSONException e) {

			}
			keyValuePairs.put(
					key,
					json.get(key));
		}
	}

	private Object[] jsonArrayToArray(
			JSONArray jsonArray ) {
		int jsonArrayLenth = jsonArray.length();
		Object[] outArray = new Object[jsonArrayLenth];
		for (int i = 0; i < jsonArrayLenth; i++) {
			outArray[i] = jsonArray.get(i);
		}
		return outArray;
	}

	private List<Object> jsonArrayToList(
			JSONArray jsonArray ) {
		int jsonArrayLenth = jsonArray.length();
		List<Object> outList = new ArrayList<Object>();
		for (int i = 0; i < jsonArrayLenth; i++) {
			outList.add(jsonArray.get(i));
		}
		return outList;
	}

}
