package mil.nga.giat.geowave.core.store.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.Post;
import org.shaded.restlet.data.Status;

import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

@GeowaveOperation(name = "cpindex", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Copy and modify existing index configuration")
public class CopyIndexCommand extends
		DefaultOperation implements
		Command
{
	private static int SUCCESS = 0;
	private static int USAGE_ERROR = -1;
	private static int INDEX_EXISTS = -2;

	@Parameter(description = "<name> <new name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default index creating stores")
	private Boolean makeDefault;

	@ParametersDelegate
	private IndexPluginOptions newPluginOptions = new IndexPluginOptions();

	private File configFile;
	private Properties existingProps;

	@Override
	public boolean prepare(
			OperationParams params ) {

		configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		existingProps = ConfigOptions.loadProperties(
				configFile,
				null);

		// Load the old index, so that we can override the values
		String oldIndex = null;
		if (parameters.size() >= 1) {
			oldIndex = parameters.get(0);
			if (!newPluginOptions.load(
					existingProps,
					IndexPluginOptions.getIndexNamespace(oldIndex))) {
				throw new ParameterException(
						"Could not find index: " + oldIndex);
			}
		}

		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			OperationParams params ) {

		// String result = computeResults(params);
		copyIndex(params);

	}

	@Post("json")
	public void computeResults() {

		String key = getQueryValue("key");
		String value = getQueryValue("value");

		if ((key == null || key.equals("")) || value == null) {
			this.setStatus(
					Status.CLIENT_ERROR_BAD_REQUEST,
					"Requires: <name> <value>");
		}
		else {
			setParameters(
					key,
					value);
			OperationParams params = new ManualOperationParams();

			params.getContext().put(
					ConfigOptions.PROPERTIES_FILE_CONTEXT,
					ConfigOptions.getDefaultPropertyFile());

			try {
				copyIndex(params);
			}
			catch (WritePropertiesException | ParameterException e) {
				this.setStatus(
						Status.SERVER_ERROR_INTERNAL,
						e.getMessage());
			}
		}
	}

	/**
	 * copies index
	 * 
	 * @return none
	 */
	private void copyIndex(
			OperationParams params ) {

		if (parameters.size() < 2) {
			throw new ParameterException(
					"Must specify <existing index> <new index> names");
		}

		// This is the new index name.
		String newIndex = parameters.get(1);
		String newIndexNamespace = IndexPluginOptions.getIndexNamespace(newIndex);

		// Make sure we're not already in the index.
		IndexPluginOptions existPlugin = new IndexPluginOptions();
		if (existPlugin.load(
				existingProps,
				newIndexNamespace)) {
			throw new ParameterException(
					"That index already exists: " + newIndex);
		}

		// Save the options.
		newPluginOptions.save(
				existingProps,
				newIndexNamespace);

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					newIndex);
		}

		// Write properties file
		if (!ConfigOptions.writeProperties(
				configFile,
				existingProps)) {
			throw new WritePropertiesException(
					"Write failure");
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String existingIndex,
			String newIndex ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(existingIndex);
		this.parameters.add(newIndex);
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
