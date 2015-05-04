package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the command-line options and parsed values specific
 * to staging intermediate data to HDFS.
 */
public class HadoopOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HadoopOptions.class);
	private final String hdfsHostPort;
	private final Path basePath;
	private final String jobTrackerHostPort;
	private final Configuration config = new Configuration();

	public HadoopOptions(
			final PropertyManagement runTimeProperties )
			throws IOException {
		final boolean setRemoteInvocation = runTimeProperties.hasProperty(MRConfig.HDFS_HOST_PORT) || runTimeProperties.hasProperty(MRConfig.JOBTRACKER_HOST_PORT);
		final String hostport = runTimeProperties.getPropertyAsString(
				MRConfig.HDFS_HOST_PORT,
				"localhost:53000");
		hdfsHostPort = (!hostport.contains("://")) ? "hdfs://" + hostport : hostport;
		basePath = new Path(
				runTimeProperties.getPropertyAsString(MRConfig.HDFS_BASE_DIR),
				"/");
		jobTrackerHostPort = runTimeProperties.getPropertyAsString(
				MRConfig.JOBTRACKER_HOST_PORT,
				runTimeProperties.getPropertyAsString(MRConfig.YARN_RESOURCE_MANAGER));

		final String name = runTimeProperties.getPropertyAsString(MapReduceParameters.MRConfig.CONFIG_FILE);

		if (name != null) {
			try (FileInputStream in = new FileInputStream(
					name)) {
				config.addResource(
						in,
						name);
			}
			catch (final IOException ex) {
				LOGGER.error(
						"Configuration file " + name + " not found",
						ex);
				throw ex;
			}
		}

		if (setRemoteInvocation) {
			GeoWaveConfiguratorBase.setRemoteInvocationParams(
					hdfsHostPort,
					jobTrackerHostPort,
					config);
		}
		else {
			LOGGER.info("Assuming local job submission");
		}
		final FileSystem fs = FileSystem.get(config);
		if (!fs.exists(basePath)) {
			LOGGER.error("HDFS base directory " + basePath + " does not exist");
			return;
		}
	}

	public HadoopOptions(
			final String hdfsHostPort,
			final Path basePath,
			final String jobTrackerHostport ) {
		this.hdfsHostPort = hdfsHostPort;
		this.basePath = basePath;
		jobTrackerHostPort = jobTrackerHostport;
	}

	public static void fillOptions(
			final Set<Option> allOptions ) {
		MapReduceParameters.fillOptions(allOptions);
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public Path getBasePath() {
		return basePath;
	}

	public String getJobTrackerOrResourceManagerHostPort() {
		return jobTrackerHostPort;
	}

	public Configuration getConfiguration() {
		return config;
	}
}
