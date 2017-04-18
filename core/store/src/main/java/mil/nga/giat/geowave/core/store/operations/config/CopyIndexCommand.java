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
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@GeowaveOperation(name = "cpindex", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Copy and modify existing index configuration")
public class CopyIndexCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<name> <new name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default index creating stores")
	private Boolean makeDefault;

	@ParametersDelegate
	private IndexPluginOptions newPluginOptions = new IndexPluginOptions();

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);

		Properties existingProps = getGeoWaveConfigProperties(params);

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

		Properties existingProps = getGeoWaveConfigProperties(params);

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
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps);

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

}
