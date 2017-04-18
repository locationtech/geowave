package mil.nga.giat.geowave.core.store.operations.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "cpstore", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Copy and modify existing store configuration")
public class CopyStoreCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<name> <new name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default store in all operations")
	private Boolean makeDefault;

	@ParametersDelegate
	private DataStorePluginOptions newPluginOptions = new DataStorePluginOptions();

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);

		Properties existingProps = getGeoWaveConfigProperties(params);

		// Load the old store, so that we can override the values
		String oldStore = null;
		if (parameters.size() >= 1) {
			oldStore = parameters.get(0);
			if (!newPluginOptions.load(
					existingProps,
					DataStorePluginOptions.getStoreNamespace(oldStore))) {
				throw new ParameterException(
						"Could not find store: " + oldStore);
			}
		}

		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			OperationParams params ) {

		Properties existingProps = getGeoWaveConfigProperties(params);

		if (parameters.size() < 2) {
			throw new ParameterException(
					"Must specify <existing store> <new store> names");
		}

		// This is the new store name.
		String newStore = parameters.get(1);
		String newStoreNamespace = DataStorePluginOptions.getStoreNamespace(newStore);

		// Make sure we're not already in the index.
		DataStorePluginOptions existPlugin = new DataStorePluginOptions();
		if (existPlugin.load(
				existingProps,
				newStoreNamespace)) {
			throw new ParameterException(
					"That store already exists: " + newStore);
		}

		// Save the options.
		newPluginOptions.save(
				existingProps,
				newStoreNamespace);

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					newStore);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps);

	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String existingStore,
			String newStore ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(existingStore);
		this.parameters.add(newStore);
	}
}
