package mil.nga.giat.geowave.ingest.hdfs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class HdfsCommandLineOptions
{
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
				"hdfs",
				true,
				"HDFS hostname and port in the format hostname:port");
		allOptions.addOption(
				"hdfs-base",
				"hdfs-base",
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
			final CommandLine commandLine ) {
		final String hdfsHostPort = commandLine.getOptionValue("hdfs");
		final String basePath = commandLine.getOptionValue("hdfs-base");
		return new HdfsCommandLineOptions(
				hdfsHostPort,
				basePath);
	}
}
