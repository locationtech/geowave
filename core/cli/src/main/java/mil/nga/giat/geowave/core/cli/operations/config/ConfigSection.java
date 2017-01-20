package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "config", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Commands that affect local configuration only")
public class ConfigSection implements
		Operation
{

	private final static Logger LOGGER = LoggerFactory.getLogger(ConfigSection.class);

	/**
	 * This method will attempt to load the config options from the given config
	 * file. If it can't find it, it will try to create it. It will then set the
	 * contextual variables 'properties' and 'properties-file', which can be
	 * used by commands to overwrite/update the properties.
	 */
	@Override
	public boolean prepare(
			OperationParams params ) {

		File propertyFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		File propertyPath = propertyFile.getParentFile();
		if (propertyPath != null && !propertyPath.exists()) {
			if (!propertyPath.mkdir()) {
				LOGGER.error("Could not create property cache path: " + propertyPath);
				return false;
			}
		}

		if (!propertyFile.exists()) {
			// Attempt to create it.
			try {
				if (!propertyFile.createNewFile()) {
					LOGGER.error("Could not create property cache file: " + propertyFile);
					return false;
				}
			}
			catch (IOException e) {
				LOGGER.error(
						"Could not create property cache file: " + propertyFile,
						e);
				return false;
			}
		}

		// Continue executing.
		return true;
	}

}
