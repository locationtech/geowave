package mil.nga.giat.geowave.service.rest.field;

import java.util.Arrays;
import java.util.List;

import org.restlet.data.Form;

public class RequestParametersForm extends
		RequestParameters
{

	public RequestParametersForm(
			Form form ) {
		super();
		for (String key : form.getNames()) {
			// For each parameter in the form, add the parameter name and value
			// to the Map<String, Object>.
			keyValuePairs.put(
					key,
					form.getFirst(
							key).getValue());
		}
	}

	@Override
	public String getString(
			String parameter ) {
		return (String) getValue(parameter);
	}

	@Override
	public List<?> getList(
			String parameter ) {
		return Arrays.asList(splitStringParameter(parameter));
	}

	@Override
	public Object[] getArray(
			String parameter ) {
		return splitStringParameter(parameter);
	}
}
