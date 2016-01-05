package mil.nga.giat.geowave.core.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * This is the primary entry point for command line tools. When run it will
 * expect an operation is specified, and will use the appropriate command-line
 * driver for the chosen operation.
 *
 */
public class GeoWaveMain
{

	@Parameter(names = "--help", description = "Display help", help = true)
	private boolean help;

	public static void main(
			final String[] args ) {
		System.exit(
				run(
						args));
	}

	public static int run(
			final String[] args ) {
		final GeoWaveMain mainArgs = new GeoWaveMain();
		final JCommander commander = new JCommander(
				mainArgs);
		commander.setProgramName("geowave");
		final OperationRegistry operationRegistry = getOperationRegistry();
		operationRegistry.initCommander(
				commander);
		commander.setAcceptUnknownOptions(
				true);
		commander.parse(
				args);
		if (mainArgs.help) {
			commander.usage();
			return 0;
		}

		return operationRegistry.run(
				commander) ? 0 : -1;
	}

	private static OperationRegistry operationRegistry = null;

	private static synchronized OperationRegistry getOperationRegistry() {
		if (operationRegistry == null) {
			operationRegistry = OperationRegistry.loadRegistry();
		}
		return operationRegistry;
	}
}
