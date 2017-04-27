package mil.nga.giat.geowave.core.store.operations.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexGroupPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@GeowaveOperation(name = "addindexgrp", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create an index group for usage in GeoWave")
public class AddIndexGroupCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<name> <comma separated list of indexes>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params ) {

		Properties existingProps = getGeoWaveConfigProperties(params);

		if (parameters.size() < 2) {
			throw new ParameterException(
					"Must specify index group name and index names (comma separated)");
		}

		// New index group name
		String newGroupName = parameters.get(0);
		String[] indexes = parameters.get(
				1).split(
				",");

		// Make sure the existing group doesn't exist.
		IndexGroupPluginOptions groupOptions = new IndexGroupPluginOptions();
		if (groupOptions.load(
				existingProps,
				getNamespace())) {
			throw new ParameterException(
					"That index group already exists: " + newGroupName);
		}

		// Make sure all the indexes exist, and add them to the group options.
		for (int i = 0; i < indexes.length; i++) {
			indexes[i] = indexes[i].trim();
			IndexPluginOptions options = new IndexPluginOptions();
			if (!options.load(
					existingProps,
					IndexPluginOptions.getIndexNamespace(indexes[i]))) {
				throw new ParameterException(
						"That index does not exist: " + indexes[i]);
			}
			groupOptions.getDimensionalityPlugins().put(
					indexes[i],
					options);
		}

		// Save the group
		groupOptions.save(
				existingProps,
				getNamespace());

		// Write to disk.
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps);
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return IndexGroupPluginOptions.getIndexGroupNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String name,
			String commaSeparatedIndexes ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(name);
		this.parameters.add(commaSeparatedIndexes);
	}

}
