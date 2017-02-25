package mil.nga.giat.geowave.core.store.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

/**
 * Common code for removing an entry from the properties file.
 */
public abstract class AbstractRemoveCommand extends
		DefaultOperation
{

	@Parameter(description = "<name>", required = true, arity = 1)
	private List<String> parameters = new ArrayList<String>();

	public String getEntryName() {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify entry name to delete");
		}

		return parameters.get(
				0).trim();
	}

	protected void computeResults(
			OperationParams params,
			String pattern ) {

		File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Load all properties
		Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		// Find properties to remove
		Set<String> keysToRemove = new HashSet<String>();
		for (String key : existingProps.stringPropertyNames()) {
			if (key.startsWith(pattern)) {
				keysToRemove.add(key);
			}
		}

		// Remove each property.
		for (String key : keysToRemove) {
			existingProps.remove(key);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);
	}

	public void setEntryName(
			String entryName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(entryName);
	}
}
