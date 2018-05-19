package mil.nga.giat.geowave.service.rest.field;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.service.rest.exceptions.UnsupportedMediaTypeException;

public class RequestParameters
{

	private Map<String, Object> keyValuePairs;
	private Constructor<Form> formConstructor;

	public RequestParameters() {
		keyValuePairs = new HashMap<String, Object>();
	}

	public RequestParameters(
			Class<Form> formClass )
			throws NoSuchMethodException,
			SecurityException {
		this();
		formConstructor = formClass.getConstructor(Representation.class);
	}

	/**
	 * Inject the APPLICATION_WWW_FORM or APPLICATION_JSON parameters from the
	 * request. Other media types throw UnsupportedMediaTypeException, which
	 * should be handled to return a 415 to the user.
	 * 
	 * @param request
	 *            The request object to pull parameters from.
	 * @throws IOException
	 * @throws UnsupportedMediaTypeException
	 */
	public void inject(
			Representation request )
			throws IOException,
			UnsupportedMediaTypeException {
		if (request == null) {
			// Is this right? Under what circumstances would the request entity
			// be null?
			throw new UnsupportedMediaTypeException();
		}
		else if (request.getMediaType().isCompatible(
				MediaType.APPLICATION_JSON)) {
			injectJsonParams(request.getText());
		}
		else if (request.getMediaType().isCompatible(
				MediaType.APPLICATION_WWW_FORM)) {
			injectFormParams(new Form(
					request));
		}
		else {
			throw new UnsupportedMediaTypeException();
		}
	}

	/**
	 * Returns the specified parameter. Execute inject(Representation request)
	 * first.
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

	private void injectFormParams(
			Form form ) {
		for (String key : form.getNames()) {
			keyValuePairs.put(
					key,
					form.getFirst(key));
		}
	}

	private void injectJsonParams(
			String jsonString ) {
		JSONObject json = new JSONObject(
				jsonString);
		for (String key : json.keySet()) {
			try {
				JSONArray arr = json.getJSONArray(key);
				List<String> injectedList = new ArrayList<String>();
				for (int i = 0; i < arr.length(); i++) {
					injectedList.add(arr.getString(i));
				}
				keyValuePairs.put(
						key,
						injectedList);
				continue;
			}
			catch (JSONException e) {

			}
			keyValuePairs.put(
					key,
					json.get(key));
		}
	}

}
