package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ComparisonCommandLineOptions
{
	private static final String TIME_ATTRIBUTE_KEY = "timeAttribute";
	private final String timeAttribute;

	public ComparisonCommandLineOptions(
			final String timeAttribute ) {
		this.timeAttribute = timeAttribute;
	}

	public String getTimeAttribute() {
		return timeAttribute;
	}

	public static ComparisonCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String timeAttribute = commandLine.getOptionValue(TIME_ATTRIBUTE_KEY);
		return new ComparisonCommandLineOptions(
				timeAttribute);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option timeAttributeOption = new Option(
				TIME_ATTRIBUTE_KEY,
				true,
				"The name of the time attribute");
		allOptions.addOption(timeAttributeOption);
	}

}
