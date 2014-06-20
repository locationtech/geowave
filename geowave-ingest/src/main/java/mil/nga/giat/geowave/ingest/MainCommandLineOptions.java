package mil.nga.giat.geowave.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class MainCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(MainCommandLineOptions.class);

	private static enum Operation {
		LOCAL_INGEST,
		STAGE_TO_HDFS,
		INGEST_FROM_HDFS,
		LOCAL_TO_HDFS_INGEST,
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
		Operation operation;
		if (commandLine.hasOption("local-ingest")) {
			operation = Operation.LOCAL_INGEST;
		}
		else if (commandLine.hasOption("hdfs-stage")) {
			operation = Operation.STAGE_TO_HDFS;
		}
		else if (commandLine.hasOption("post-stage")) {
			operation = Operation.INGEST_FROM_HDFS;
		}
		else if (commandLine.hasOption("hdfs-ingest")) {
			operation = Operation.LOCAL_TO_HDFS_INGEST;
		}
		else {
			LOGGER.fatal("Operation not set.  One of 'local-ingest', 'hdfs-stage', 'post-stage', and 'hdfs-ingest' must be provided");
			throw new IllegalArgumentException(
					"Operation not set.  One of 'local-ingest', 'hdfs-stage', 'post-stage', and 'hdfs-ingest' must be provided");
		}
		return new MainCommandLineOptions(
				operation);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final OptionGroup operationChoice = new OptionGroup();
		operationChoice.setRequired(true);
		operationChoice.addOption(new Option(
				"local-ingest",
				"ingest supported files in local file system directly, without using HDFS"));
		operationChoice.addOption(new Option(
				"hdfs-stage",
				"stage supported files in local file system to HDFS"));
		operationChoice.addOption(new Option(
				"post-stage",
				"ingest supported files that already exist in HDFS"));
		operationChoice.addOption(new Option(
				"hdfs-ingest",
				"copy supported files from local file system to HDFS and ingest from HDFS"));
	}
}
