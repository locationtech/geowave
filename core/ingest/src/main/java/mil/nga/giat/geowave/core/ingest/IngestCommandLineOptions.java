package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class IngestCommandLineOptions
{
	private final String visibility;
	private final boolean clearNamespace;
	private final String dimensionalityType;

	public IngestCommandLineOptions(
			final String visibility,
			final boolean clearNamespace,
			final String dimensionalityType ) {
		this.visibility = visibility;
		this.clearNamespace = clearNamespace;
		this.dimensionalityType = dimensionalityType;
	}

	public String getVisibility() {
		return visibility;
	}

	public String[] getDimensionalityTypes() {
		if ((dimensionalityType != null) && dimensionalityType.contains(",")) {
			return dimensionalityType.split(",");
		}
		return new String[] {
			dimensionalityType
		};
	}

	public String getDimensionalityTypeArgument() {
		return dimensionalityType;
	}

	public boolean isClearNamespace() {
		return clearNamespace;
	}

	public static IngestCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final boolean success = true;
		boolean clearNamespace = false;
		if (commandLine.hasOption("c")) {
			clearNamespace = true;
		}
		String visibility = null;
		if (commandLine.hasOption("v")) {
			visibility = commandLine.getOptionValue("v");
		}
		final String dimensionalityType = commandLine.getOptionValue(
				"dim",
				IngestUtils.getDefaultDimensionalityType());
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		return new IngestCommandLineOptions(
				visibility,
				clearNamespace,
				dimensionalityType);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visibility of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		final Option dimensionalityType = new Option(
				"dim",
				"dimensionality",
				true,
				"The preferred dimensionality type to index the data for this ingest operation. Can be a comma-delimited set to ingest into multiple dimensionalities within the same ingest operation.  " + IngestUtils.getDimensionalityTypeOptionDescription());
		allOptions.addOption(dimensionalityType);
		allOptions.addOption(new Option(
				"c",
				"clear",
				false,
				"Clear ALL data stored with the same prefix as this namespace (optional; default is to append data to the namespace if it exists)"));
	}
}
