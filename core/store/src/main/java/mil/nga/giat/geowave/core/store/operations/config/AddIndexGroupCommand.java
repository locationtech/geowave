package mil.nga.giat.geowave.core.store.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.shaded.restlet.data.Form;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.Post;
import org.shaded.restlet.data.Status;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.annotations.RestParameters;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexGroupPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

@GeowaveOperation(name = "addindexgrp", parentOperation = ConfigSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "Create an index group for usage in GeoWave")
public class AddIndexGroupCommand extends
		DefaultOperation<Void> implements
		Command
{
	private static int SUCCESS = 0;
	private static int USAGE_ERROR = -1;
	private static int INDEXING_ERROR = -2;
	private static int GROUP_EXISTS = -3;

	@Parameter(description = "<name> <comma separated list of indexes>")
	@RestParameters(names = {
		"key",
		"value"
	})
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params ) {
		addIndexGroup(params);
	}

	/**
	 * Add rest endpoint for the addIndexGroup command. Looks for POST params
	 * with keys 'key' and 'value' to set.
	 * 
	 * @return none
	 */
	@Override
	public Void computeResults(
			OperationParams params ) {

		try {
			addIndexGroup(params);
		}
		catch (WritePropertiesException | ParameterException e) {
			this.setStatus(
					Status.SERVER_ERROR_INTERNAL,
					e.getMessage());
		}

		return null;
	}

	/**
	 * Adds index group
	 * 
	 * @parameters params
	 * @return none
	 */
	private void addIndexGroup(
			OperationParams params ) {
		File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

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
		if (!ConfigOptions.writeProperties(
				propFile,
				existingProps)) {
			throw new WritePropertiesException(
					"Write failure");
		}
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
