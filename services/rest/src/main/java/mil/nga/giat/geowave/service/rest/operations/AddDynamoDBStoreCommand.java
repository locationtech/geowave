package mil.nga.giat.geowave.service.rest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.exceptions.DuplicateEntryException;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

@GeowaveOperation(name = "addstore/dynamodb", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a store within Geowave")
public class AddDynamoDBStoreCommand extends
		ServiceEnabledCommand<String>
{
	/**
	 * A REST Operation for the AddStoreCommand where --type=dynamodb
	 */
	public static final String PROPERTIES_CONTEXT = "properties";

	// Default AddStore Options
	@Parameter(description = "<name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default store in all operations")
	private Boolean makeDefault;

	private DataStorePluginOptions pluginOptions = new DataStorePluginOptions();

	@ParametersDelegate
	private DynamoDBOptions opts;

	@Override
	public boolean prepare(
			final OperationParams params ) {

		pluginOptions.selectPlugin("dynamodb");
		pluginOptions.setFactoryOptions(opts);
		return true;
	}

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		computeResults(params);
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {

		final File propFile = getGeoWaveConfigFile(params);

		final Properties existingProps = ConfigOptions.loadProperties(propFile);

		// Ensure that a name is chosen.
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		// Make sure we're not already in the index.
		final DataStorePluginOptions existingOptions = new DataStorePluginOptions();
		if (existingOptions.load(
				existingProps,
				getNamespace())) {
			throw new DuplicateEntryException(
					"That store already exists: " + getPluginName());
		}

		// Save the store options.
		pluginOptions.save(
				existingProps,
				getNamespace());

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					getPluginName());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);

		StringBuilder builder = new StringBuilder();
		for (Object key : existingProps.keySet()) {
			String[] split = key.toString().split(
					"\\.");
			if (split.length > 1) {
				if (split[1].equals(parameters.get(0))) {
					builder.append(key.toString() + "=" + existingProps.getProperty(key.toString()) + "\n");
				}
			}
		}
		return builder.toString();
	}

	@Override
	public String getId() {
		return ConfigSection.class.getName() + ".addstore/dynamodb";
	}

	@Override
	public String getPath() {
		return "v0/config/addstore/dynamodb";
	}

	public DataStorePluginOptions getPluginOptions() {
		return pluginOptions;
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return DataStorePluginOptions.getStoreNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
	}

	public Boolean getMakeDefault() {
		return makeDefault;
	}

	public void setMakeDefault(
			final Boolean makeDefault ) {
		this.makeDefault = makeDefault;
	}

	public String getStoreType() {
		return "dynamodb";
	}

	public void setPluginOptions(
			final DataStorePluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}
