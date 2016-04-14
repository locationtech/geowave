package mil.nga.giat.geowave.core.cli.api;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;

/**
 * A special instance of OperationParams that designates that JCommander was
 * used to populate the parameters.
 */
public interface JCommanderOperationParams extends
		OperationParams
{
	/**
	 * Retrieve the commander that was used to parse the command line.
	 * 
	 * @return
	 */
	JCommander getCommander();

	/**
	 * Get the translation map that was used to translate between the Operation
	 * objects and the facade objects passed to jcommander.
	 * 
	 * @return
	 */
	JCommanderTranslationMap getTranslationMap();
}
