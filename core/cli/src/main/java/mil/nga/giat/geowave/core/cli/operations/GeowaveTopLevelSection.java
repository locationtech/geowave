package mil.nga.giat.geowave.core.cli.operations;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "geowave")
@Parameters(commandDescription = "This is the top level section.")
public class GeowaveTopLevelSection implements
		Operation
{

	@Parameter(names = "--version", description = "Output Geowave build version information")
	private Boolean versionFlag;

	@Parameter(names = "--help", hidden = true, description = "Get help on specific GeoWave commands. Same as the 'help' command.")
	private Boolean help;

	// This contains methods and parameters for determining where the GeoWave
	// cached configuration file is.
	@ParametersDelegate
	private ConfigOptions options = new ConfigOptions();

	@Override
	public boolean prepare(
			final OperationParams inputParams ) {

		// Print out the version info if requested.
		if (Boolean.TRUE.equals(versionFlag)) {
			VersionUtils.printVersionInfo();
			// Do not continue
			return false;
		}

		// If help is enabled, modify the params
		if (Boolean.TRUE.equals(help)) {
			JCommander.getConsole().println(
					"Use 'geowave help <command>' to get " + "detailed help, or 'geowave explain <command>' to discover missing parameters.");
			return false;
		}

		// This will load the properties file parameter into the
		// operation params.
		options.prepare(inputParams);

		// Successfully prepared
		return true;
	}
}
