package mil.nga.giat.geowave.core.ingest.local;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class encapsulates all of the options and parsed values specific to
 * directing the ingestion framework to a local file system. The user must set
 * an input file or directory and can set a list of extensions to narrow the
 * ingestion to. The process will recurse a directory and filter by the
 * extensions if provided.
 */
public class LocalInputCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(LocalInputCommandLineOptions.class);
	private final String input;
	private final String[] extensions;

	public LocalInputCommandLineOptions(
			final String input,
			final String[] extensions ) {
		this.input = input;
		this.extensions = extensions;
	}

	public String getInput() {
		return input;
	}

	public String[] getExtensions() {
		return extensions;
	}

	public static LocalInputCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		String value = null;
		if (commandLine.hasOption("b")) {
			value = commandLine.getOptionValue("b");
		}
		else {
			throw new ParseException(
					"Unable to ingest data, input file or base directory not specified");
		}
		String[] extensions = null;

		if (commandLine.hasOption("x")) {
			try {
				extensions = commandLine.getOptionValue(
						"x").split(
						",");

			}
			catch (final Exception ex) {
				LOGGER.warn(
						"Error parsing extensions argument, ignoring file extension option",
						ex);
			}
		}
		return new LocalInputCommandLineOptions(
				value,
				extensions);
	}

	public static void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(new Option(
				"b",
				"base",
				true,
				"Base input file or directory to crawl with one of the supported ingest types"));

		allOptions.addOption(
				"x",
				"extension",
				true,
				"individual or comma-delimited set of file extensions to accept (optional)");
	}
}
