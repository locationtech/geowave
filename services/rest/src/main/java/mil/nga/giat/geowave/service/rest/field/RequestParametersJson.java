package mil.nga.giat.geowave.service.rest.field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.representation.Representation;

public class RequestParametersJson extends
		RequestParameters
{

	public RequestParametersJson(
			Representation request )
			throws IOException {
		super();
		injectJsonParams(request.getText());
	}

	@Override
	public String getString(
			String parameter ) {
		return (String) getValue(parameter);
	}

	@Override
	public List<?> getList(
			String parameter ) {
		return jsonArrayToList((JSONArray) getValue(parameter));
	}

	@Override
	public Object[] getArray(
			String parameter ) {
		return jsonArrayToArray((JSONArray) getValue(parameter));
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
