package mil.nga.giat.geowave.core.cli;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the primary entry point for command line tools. When run it will
 * expect an operation is specified, and will use the appropriate command-line
 * driver for the chosen operation.
 * 
 */
public class GeoWaveMain
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveMain.class);

	public static void main(
			final String[] args ) {
		if (args.length < 1) {
			OperationCommandLineOptions.printHelp();
		}

		final Options operations = new Options();

		OperationCommandLineOptions.applyOptions(operations);

		final String[] optionsArgs = new String[args.length - 1];
		System.arraycopy(
				args,
				1,
				optionsArgs,
				0,
				optionsArgs.length);
		final String[] operationsArgs = new String[] {
			args[0]
		};
		final Parser parser = new BasicParser();
		CommandLine operationCommandLine;
		try {
			operationCommandLine = parser.parse(
					operations,
					operationsArgs);
			final OperationCommandLineOptions operationOption = OperationCommandLineOptions.parseOptions(operationCommandLine);
			operationOption.getOperation().getDriver().run(
					optionsArgs);
		}
		catch (final ParseException e) {
			LOGGER.error(
					"Unable to parse operation",
					e);
			System.exit(-1);
		}
	}
}
