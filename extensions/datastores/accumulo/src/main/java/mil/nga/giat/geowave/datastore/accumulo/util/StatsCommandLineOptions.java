package mil.nga.giat.geowave.datastore.accumulo.util;

import mil.nga.giat.geowave.datastore.accumulo.AccumuloCommandLineOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloCommandLineOptions.class);
	private final String typeName;
	private final String authorizations;

	public StatsCommandLineOptions(
			final String typeName,
			final String authorizations ) {
		this.typeName = typeName;
		this.authorizations = authorizations;

	}

	public String getTypeName() {
		return typeName;
	}

	public String getAuthorizations() {
		return authorizations;
	}

	public static StatsCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String type = commandLine.getOptionValue("type");
		final String auth = commandLine.getOptionValue(
				"auth",
				"");
		return new StatsCommandLineOptions(
				type,
				auth);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option type = new Option(
				"type",
				true,
				"The name of the feature type to run stats on");
		type.setRequired(true);
		allOptions.addOption(type);

		final Option auth = new Option(
				"auth",
				true,
				"The authorizations used for the statistics calculation as a subset of the accumulo user authorization; by default all authorizations are used.");
		auth.setRequired(false);
		allOptions.addOption(auth);
	}
}
