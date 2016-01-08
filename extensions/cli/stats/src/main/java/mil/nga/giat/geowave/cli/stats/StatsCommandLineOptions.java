package mil.nga.giat.geowave.cli.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class StatsCommandLineOptions
{
	private final String adapterId;
	private final String authorizations;

	public StatsCommandLineOptions(
			final String adapterId,
			final String authorizations ) {
		this.adapterId = adapterId;
		this.authorizations = authorizations;
	}

	public String getAdapterId() {
		return adapterId;
	}

	public String getAuthorizations() {
		return authorizations;
	}

	public static StatsCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String adapterId = commandLine.getOptionValue("adapterId");
		final String auth = commandLine.getOptionValue(
				"auth",
				"");
		return new StatsCommandLineOptions(
				adapterId,
				auth);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option adapterIdOption = new Option(
				"adapterId",
				true,
				"The ID of the adapter to run stats on. The default is to use all adapters. For features this is the feature type name and for grid coverages this is coverage name.");
		adapterIdOption.setRequired(false);
		allOptions.addOption(adapterIdOption);

		final Option auth = new Option(
				"auth",
				true,
				"The authorizations used for the statistics calculation as a subset of the accumulo user authorization; by default all authorizations are used.");
		auth.setRequired(false);
		allOptions.addOption(auth);
	}
}
