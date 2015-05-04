package mil.nga.giat.geowave.core.ingest.hdfs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class encapsulates the command-line options and parsed values specific
 * to staging intermediate data to HDFS.
 */
public class HdfsCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(HdfsCommandLineOptions.class);
	private final String hdfsHostPort;
	private final String basePath;

	public HdfsCommandLineOptions(
			final String hdfsHostPort,
			final String basePath ) {
		this.hdfsHostPort = hdfsHostPort;
		this.basePath = basePath;
	}

	public static void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(
				"hdfs",
				true,
				"HDFS hostname and port in the format hostname:port");
		allOptions.addOption(
				"hdfsbase",
				true,
				"fully qualified path to the base directory in hdfs");
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getBasePath() {
		return basePath;
	}

	public static HdfsCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		String hdfsHostPort = commandLine.getOptionValue("hdfs");
		final String basePath = commandLine.getOptionValue("hdfsbase");
		boolean success = true;
		if (hdfsHostPort == null) {
			success = false;
			LOGGER.fatal("HDFS host:port not set");
		}
		if (basePath == null) {
			success = false;
			LOGGER.fatal("HDFS base path not set");
		}
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}

		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		return new HdfsCommandLineOptions(
				hdfsHostPort,
				basePath);
	}
}
