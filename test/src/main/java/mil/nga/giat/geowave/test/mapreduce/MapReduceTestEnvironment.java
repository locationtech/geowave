package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

abstract public class MapReduceTestEnvironment extends
		GeoWaveTestEnvironment
{
	private final static Logger LOGGER = Logger.getLogger(MapReduceTestEnvironment.class);

	protected static final String HDFS_BASE_DIRECTORY = "test_tmp";
	protected static final String DEFAULT_JOB_TRACKER = "local";
	protected static final String EXPECTED_RESULTS_KEY = "EXPECTED_RESULTS";
	protected static final int MIN_INPUT_SPLITS = 2;
	protected static final int MAX_INPUT_SPLITS = 4;
	protected static String jobtracker;
	protected static String hdfs;
	protected static boolean hdfsProtocol;
	protected static String hdfsBaseDirectory;

	protected void testMapReduceIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest gpx data directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		String[] args = null;
		synchronized (MUTEX) {
			args = StringUtils.split(
					"-hdfsingest -f gpx -hdfs " + hdfs + " -hdfsbase " + hdfsBaseDirectory + " -jobtracker " + jobtracker + " -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
					' ');
		}
		GeoWaveMain.main(args);
	}

	@BeforeClass
	public static void setVariables()
			throws IOException {
		GeoWaveTestEnvironment.setup();
		hdfs = System.getProperty("hdfs");
		jobtracker = System.getProperty("jobtracker");
		if (!isSet(hdfs)) {
			hdfs = "file:///";

			hdfsBaseDirectory = TEMP_DIR.toURI().toURL().toString() + "/" + HDFS_BASE_DIRECTORY;
			hdfsProtocol = false;
		}
		else {
			hdfsBaseDirectory = HDFS_BASE_DIRECTORY;
			if (!hdfs.contains("://")) {
				hdfs = "hdfs://" + hdfs;
				hdfsProtocol = true;
			}
			else {
				hdfsProtocol = hdfs.toLowerCase().startsWith(
						"hdfs://");
			}
		}
		if (!isSet(jobtracker)) {
			jobtracker = DEFAULT_JOB_TRACKER;
		}
	}

	@AfterClass
	public static void cleanupHdfsFiles() {
		if (hdfsProtocol) {
			final Path tmpDir = new Path(
					hdfsBaseDirectory);
			try {
				final FileSystem fs = FileSystem.get(getConfiguration());
				fs.delete(
						tmpDir,
						true);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to delete HDFS temp directory",
						e);
			}
		}
	}

	public static void filterConfiguration(
			final Configuration conf ) {
		// final parameters, can't be overriden
		conf.unset("mapreduce.job.end-notification.max.retry.interval");
		conf.unset("mapreduce.job.end-notification.max.attempts");

		// deprecated parameters (added in by default since we used the
		// Configuration() constructor (everything is set))
		conf.unset("session.id");
		conf.unset("mapred.jar");
		conf.unset("fs.default.name");
		conf.unset("mapred.map.tasks.speculative.execution");
		conf.unset("mapred.reduce.tasks");
		conf.unset("mapred.reduce.tasks.speculative.execution");
		conf.unset("mapred.mapoutput.value.class");
		conf.unset("mapred.used.genericoptionsparser");
		conf.unset("mapreduce.map.class");
		conf.unset("mapred.job.name");
		conf.unset("mapreduce.inputformat.class");
		conf.unset("mapred.input.dir");
		conf.unset("mapreduce.outputformat.class");
		conf.unset("mapred.map.tasks");
		conf.unset("mapred.mapoutput.key.class");
		conf.unset("mapred.working.dir");
	}

	protected static Configuration getConfiguration() {
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfs);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(
				"mapreduce.jobtracker.address",
				jobtracker);
		// for travis-ci to run, we want to limit the memory consumption
		conf.setInt(
				MRJobConfig.IO_SORT_MB,
				10);

		filterConfiguration(conf);

		return conf;

	}

}
