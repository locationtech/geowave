package mil.nga.giat.geowave.datastore.accumulo.split;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitCommandLineOptions
{
	private static Logger LOGGER = LoggerFactory.getLogger(SplitCommandLineOptions.class);
	private final String indexId;
	private final long number;

	public SplitCommandLineOptions(
			final String indexId,
			final long number ) {
		this.indexId = indexId;
		this.number = number;
	}

	public String getIndexId() {
		return indexId;
	}

	public long getNumber() {
		return number;
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option num = new Option(
				"num",
				true,
				"The number of partitions (or entries)");
		num.setRequired(true);
		allOptions.addOption(num);
		final Option indexId = new Option(
				"indexId",
				true,
				"The geowave index ID (optional; default is all indices)");
		indexId.setRequired(false);
		allOptions.addOption(indexId);
	}

	public static SplitCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String indexId = commandLine.getOptionValue("indexId");
		final String num = commandLine.getOptionValue("num");
		final long number = Long.parseLong(num);
		return new SplitCommandLineOptions(
				indexId,
				number);
	}
}
