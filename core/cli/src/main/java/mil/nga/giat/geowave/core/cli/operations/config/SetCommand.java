package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.data.Form;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.Post;
import org.shaded.restlet.data.Status;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

@GeowaveOperation(name = "set", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Set property name within cache")
public class SetCommand extends
		DefaultOperation implements
		Command
{

	private static int SUCCESS = 0;
	private static int USAGE_ERROR = -1;
	private static int WRITE_FAILURE = -2;

	@Parameter(description = "<name> <value>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params ) {
		setKeyValue(params);
	}

	/**
	 * Add rest endpoint for the set command. Looks for GET params with keys
	 * 'key' and 'value' to set.
	 * 
	 * @return string containing json with details of success or failure of the
	 *         set
	 */
	@Post("form:json")
	public Object computeResults(
			Representation entity ) {
		Form form = new Form(
				entity);
		String key = form.getFirstValue("key");
		String value = form.getFirstValue("value");
		String configFileParameter = form.getFirstValue("config_file");
		File configFile = (configFileParameter != null) ? new File(
				configFileParameter) : ConfigOptions.getDefaultPropertyFile();
		// String key = getQueryValue("key");
		// String value = getQueryValue("value");

		if ((key == null || key.equals("")) || value == null) {
			this.setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"Requires: <name> <value>");
			return null;
		}

		setParameters(
				key,
				value);
		OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		try {
			return setKeyValue(params);
		}
		catch (WritePropertiesException | ParameterException e) {
			this.setStatus(
					Status.SERVER_ERROR_INTERNAL,
					e.getMessage());
			return null;
		}
	}

	/**
	 * Set the key value pair in the config. Store the previous value of the key
	 * in prevValue
	 */
	private Object setKeyValue(
			OperationParams params ) {

		File f = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties p = ConfigOptions.loadProperties(
				f,
				null);

		String key = null;
		String value = null;
		if (parameters.size() == 1 && parameters.get(
				0).indexOf(
				"=") != -1) {
			String[] parts = StringUtils.split(
					parameters.get(0),
					"=");
			key = parts[0];
			value = parts[1];
		}
		else if (parameters.size() == 2) {
			key = parameters.get(0);
			value = parameters.get(1);
		}
		else {
			throw new ParameterException(
					"Requires: <name> <value>");
		}

		Object previousValue = p.setProperty(
				key,
				value);
		if (!ConfigOptions.writeProperties(
				f,
				p)) {
			throw new WritePropertiesException(
					"Write failure");
		}
		else {
			return previousValue;
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String key,
			String value ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(key);
		this.parameters.add(value);
	}

	private static class WritePropertiesException extends
			RuntimeException
	{

		private WritePropertiesException(
				String string ) {
			super(
					string);
		}

	}
}
