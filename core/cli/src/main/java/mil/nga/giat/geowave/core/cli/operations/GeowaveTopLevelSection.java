package mil.nga.giat.geowave.core.cli.operations;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

@GeowaveOperation(name = "geowave")
@Parameters(commandDescription = "This is the top level section.")
public class GeowaveTopLevelSection extends
		DefaultOperation
{
	@Parameter(names = "--debug", description = "Verbose output")
	private Boolean verboseFlag;

	@Parameter(names = "--version", description = "Output Geowave build version information")
	private Boolean versionFlag;

	// This contains methods and parameters for determining where the GeoWave
	// cached configuration file is.
	@ParametersDelegate
	private ConfigOptions options = new ConfigOptions();

	@Override
	public boolean prepare(
			final OperationParams inputParams ) {
		// This will load the properties file parameter into the
		// operation params.
		options.prepare(inputParams);

		super.prepare(inputParams);

		// Up the log level
		if (Boolean.TRUE.equals(verboseFlag)) {
			LogManager.getRootLogger().setLevel(
					Level.DEBUG);
		}

		// Print out the version info if requested.
		if (Boolean.TRUE.equals(versionFlag)) {
			VersionUtils.printVersionInfo();
			// Do not continue
			return false;
		}

		// Successfully prepared
		return true;
	}
}