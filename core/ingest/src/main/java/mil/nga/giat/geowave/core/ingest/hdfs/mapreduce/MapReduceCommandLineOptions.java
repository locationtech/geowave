package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import mil.nga.giat.geowave.core.ingest.hdfs.HdfsCommandLineOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class encapsulates all of the options and parsed values specific to
 * setting up the GeoWave ingestion framework to run on hadoop map-reduce.
 * Currently the only required parameter is the host name and port for the
 * hadoop job tracker.
 */
public class MapReduceCommandLineOptions
{
	private final static Logger LOGGER = Logger.getLogger(MapReduceCommandLineOptions.class);
	private final String jobTrackerHostPort;

	public MapReduceCommandLineOptions(
			final String jobTrackerHostPort ) {
		this.jobTrackerHostPort = jobTrackerHostPort;
	}

	public static void applyOptions(
			final Options allOptions ) {
		OptionGroup jobTrackerOrResourceManager = new OptionGroup();
		jobTrackerOrResourceManager.addOption(new Option(
				"jobtracker",
				true,
				"Hadoop job tracker hostname and port in the format hostname:port"));
		jobTrackerOrResourceManager.addOption(new Option(
				"resourceman",
				true,
				"Yarn resource manager hostname and port in the format hostname:port"));
		allOptions.addOptionGroup(jobTrackerOrResourceManager);
	}

	public String getJobTrackerOrResourceManagerHostPort() {
		return jobTrackerHostPort;
	}

	public static MapReduceCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		String jobTrackerHostPort = commandLine.getOptionValue("jobtracker");
		boolean success = true;
		if (jobTrackerHostPort == null) {
			jobTrackerHostPort = commandLine.getOptionValue("resourceman");
			if (jobTrackerHostPort == null) {
				success = false;
				LOGGER.fatal("Job tracker or resource manager host:port must be set");
			}
		}
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		return new MapReduceCommandLineOptions(
				jobTrackerHostPort);
	}
}
