package mil.nga.giat.geowave.core.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.operations.ExplainCommand;
import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;
import mil.nga.giat.geowave.core.cli.operations.HelpCommand;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

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

		// Take an initial stab at running geowave with the given arguments.
		OperationParser parser = new OperationParser(
				prepRegistry());
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);

		// No problems so far.
		if (params.getSuccessCode() == 0) {
			run(params);
		}

		// Log error to console if any.
		if (params.getSuccessCode() != 0) {
			doHelp(params);
			LOGGER.debug(
					params.getSuccessMessage(),
					params.getSuccessException());
			JCommander.getConsole().println(
					"\n" + params.getSuccessMessage());
		}
		else if (!params.isCommandPresent()) {
			doHelp(params);
		}

		System.exit(params.getSuccessCode());
	}

	/**
	 * Run the operations contained in CommandLineOperationParams.
	 * 
	 * @param params
	 */
	private static void run(
			CommandLineOperationParams params ) {
		// Execute the command
		for (Operation operation : params.getOperationMap().values()) {
			if (operation instanceof Command) {

				try {
					((Command) operation).execute(params);
				}
				catch (Exception p) {
					params.setSuccessCode(-1);
					params.setSuccessMessage(String.format(
							"Unable to execute operation: %s",
							p.getMessage()));
					params.setSuccessException(p);
				}

				// Only execute the first command.
				break;
			}
		}
	}

	/**
	 * This adds the help and explain commands to have all operations as
	 * children, so the user can do 'help command' or 'explain command'
	 * 
	 * @return
	 */
	private static OperationRegistry prepRegistry() {
		OperationRegistry registry = new OperationRegistry();

		OperationEntry explainCommand = registry.getOperation(ExplainCommand.class);
		OperationEntry helpCommand = registry.getOperation(HelpCommand.class);
		OperationEntry topLevel = registry.getOperation(GeowaveTopLevelSection.class);

		// Special processing for "HelpSection". This special section will be
		// added as a child to
		// top level, and will have all the same children as top level.
		for (OperationEntry entry : topLevel.getChildren()) {
			if (entry != helpCommand && entry != explainCommand) {
				helpCommand.addChild(entry);
				explainCommand.addChild(entry);
			}
		}

		return registry;
	}

	/**
	 * This function will show options for the given operation/section.
	 */
	private static void doHelp(
			CommandLineOperationParams params ) {
		HelpCommand command = new HelpCommand();
		command.execute(params);
	}
}
