package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsDriver;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestDriver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

/**
 * This class encapsulates the option for selecting the operation and parses the
 * value.
 */
public class OperationCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(OperationCommandLineOptions.class);

	/**
	 * This enumeration identifies the set of operations supported and which
	 * driver to execute based on the operation selected.
	 */
	public static enum Operation {
		CLEAR_NAMESPACE(
				"clear",
				"clear ALL data from a GeoWave namespace, this actually deletes Accumulo tables prefixed by the given namespace",
				new ClearNamespaceDriver(
						"clear")),
		LOCAL_INGEST(
				"localingest",
				"ingest supported files in local file system directly, without using HDFS",
				new LocalFileIngestDriver(
						"localingest")),
		STAGE_TO_HDFS(
				"hdfsstage",
				"stage supported files in local file system to HDFS",
				new StageToHdfsDriver(
						"hdfsstage")),
		INGEST_FROM_HDFS(
				"poststage",
				"ingest supported files that already exist in HDFS",
				new IngestFromHdfsDriver(
						"poststage")),
		LOCAL_TO_HDFS_INGEST(
				"hdfsingest",
				"copy supported files from local file system to HDFS and ingest from HDFS",
				new MultiStageCommandLineDriver(
						"hdfsingest",
						new AbstractCommandLineDriver[] {
							new StageToHdfsDriver(
									"hdfsingest"),
							new IngestFromHdfsDriver(
									"hdfsingest")
						}));

		private final String commandlineOptionValue;
		private final String description;
		private final AbstractCommandLineDriver driver;

		private Operation(
				final String commandlineOptionValue,
				final String description,
				final AbstractCommandLineDriver driver ) {
			this.commandlineOptionValue = commandlineOptionValue;
			this.description = description;
			this.driver = driver;
		}

		public String getCommandlineOptionValue() {
			return commandlineOptionValue;
		}

		public String getDescription() {
			return description;
		}

		public AbstractCommandLineDriver getDriver() {
			return driver;
		}
	}

	private final Operation operation;

	public OperationCommandLineOptions(
			final Operation operation ) {
		this.operation = operation;
	}

	public Operation getOperation() {
		return operation;
	}

	public static OperationCommandLineOptions parseOptions(
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
		return new OperationCommandLineOptions(
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
