package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;
import java.net.MalformedURLException;

import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.store.index.IndexType;
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
	protected static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
	protected static final String TEST_CASE_GENERAL_GPX_BASE = TEST_CASE_BASE + "general_gpx_test_case/";
	protected static final String GENERAL_GPX_FILTER_PACKAGE = TEST_CASE_GENERAL_GPX_BASE + "filter/";
	protected static final String GENERAL_GPX_FILTER_FILE = GENERAL_GPX_FILTER_PACKAGE + "filter.shp";
	protected static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	protected static final String GENERAL_GPX_EXPECTED_RESULTS_DIR = TEST_CASE_GENERAL_GPX_BASE + "filter_results/";
	protected static final String OSM_GPX_INPUT_DIR = TEST_CASE_BASE + "osm_gpx_test_case/";
	protected static final String HDFS_BASE_DIRECTORY = "test_tmp";
	protected static final String DEFAULT_JOB_TRACKER = "local";
	protected static final String EXPECTED_RESULTS_KEY = "EXPECTED_RESULTS";
	protected static final int MIN_INPUT_SPLITS = 2;
	protected static final int MAX_INPUT_SPLITS = 4;
	protected static String jobtracker;
	protected static String hdfs;
	protected static boolean hdfsProtocol;
	protected static String hdfsBaseDirectory;

	@BeforeClass
	public static void extractTestFiles() {
		GeoWaveTestEnvironment.unZipFile(
				MapReduceTestEnvironment.class.getClassLoader().getResourceAsStream(
						TEST_DATA_ZIP_RESOURCE_PATH),
				TEST_CASE_BASE);
	}

	protected void testIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest gpx data directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-hdfsingest -t gpx -hdfs " + hdfs + " -hdfsbase " + hdfsBaseDirectory + " -jobtracker " + jobtracker + " -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		IngestMain.main(args);
	}

	@BeforeClass
	public static void setVariables()
			throws MalformedURLException {
		GeoWaveTestEnvironment.setup();
		hdfs = System.getProperty("hdfs");
		jobtracker = System.getProperty("jobtracker");
		if (!isSet(hdfs)) {
			hdfs = "file:///";

			hdfsBaseDirectory = tempDir.toURI().toURL().toString() + "/" + HDFS_BASE_DIRECTORY;
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

	protected static Configuration getConfiguration() {
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfs);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(
				"mapred.job.tracker",
				jobtracker);
		// for travis-ci to run, we want to limit the memory consumption
		conf.setInt(
				MRJobConfig.IO_SORT_MB,
				10);
		return conf;
	}

}
