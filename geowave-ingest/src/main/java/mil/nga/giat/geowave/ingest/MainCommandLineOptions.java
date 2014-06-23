package mil.nga.giat.geowave.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class MainCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(MainCommandLineOptions.class);

	public static enum Operation {
		LOCAL_INGEST(
				"localingest",
				"ingest supported files in local file system directly, without using HDFS"),
		STAGE_TO_HDFS(
				"hdfsstage",
				"stage supported files in local file system to HDFS"),
		INGEST_FROM_HDFS(
				"poststage",
				"ingest supported files that already exist in HDFS"),
		LOCAL_TO_HDFS_INGEST(
				"hdfsingest",
				"copy supported files from local file system to HDFS and ingest from HDFS");

		private final String commandlineOptionValue;
		private final String description;

		private Operation(
				final String commandlineOptionValue,
				final String description ) {
			this.commandlineOptionValue = commandlineOptionValue;
			this.description = description;
		}

		public String getCommandlineOptionValue() {
			return commandlineOptionValue;
		}

		public String getDescription() {
			return description;
		}

	}

	private final Operation operation;

	public MainCommandLineOptions(
			final Operation operation ) {
		this.operation = operation;
	}

	public Operation getOperation() {
		return operation;
	}

	public static MainCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws IllegalArgumentException {
		Operation operation = null;
		for (final Operation o : Operation.values()) {
			if (commandLine.hasOption(o.getCommandlineOptionValue())) {
				operation = o;
				break;
			}
		}
		if (operation == null) {
			final StringBuffer str = new StringBuffer();
			for (int i = 0; i < Operation.values().length; i++) {
				final Operation o = Operation.values()[i];
				str.append(
						"'").append(
						o.commandlineOptionValue).append(
						"'");
				if (i != (Operation.values().length - 1)) {
					str.append(", ");
					if (i == (Operation.values().length - 2)) {
						str.append("and ");
					}
				}
			}
			LOGGER.fatal("Operation not set.  One of " + str.toString() + " must be provided");
			throw new IllegalArgumentException(
					"Operation not set.  One of " + str.toString() + " must be provided");
		}
		return new MainCommandLineOptions(
				operation);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final OptionGroup operationChoice = new OptionGroup();
		operationChoice.setRequired(true);
		for (final Operation o : Operation.values()) {
			operationChoice.addOption(new Option(
					o.getCommandlineOptionValue(),
					o.getDescription()));
		}
		allOptions.addOptionGroup(operationChoice);
	}
}
