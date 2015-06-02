/**
 * 
 */
package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;

import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author viggy
 * 
 */
public class GeoWaveHBaseTestEnvironment extends
		GeoWaveTestEnvironment
{

	private final static Logger LOGGER = Logger.getLogger(GeoWaveHBaseTestEnvironment.class);
	protected static BasicHBaseOperations operations;
	protected static String zookeeper;
	protected static File TEMP_DIR = new File(
			"./target/hbase_temp"); // breaks on windows if temp directory
									// isn't on same drive as project
	private static HBaseTestingUtility utilty;
	private static MiniHBaseCluster hbaseInstance;

	@BeforeClass
	public static void setup()
			throws IOException {
		synchronized (MUTEX) {
			TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
			if (operations == null) {
				zookeeper = System.getProperty("zookeeperUrl");
				if (!isSet(zookeeper)) {
					Assert.fail("Currently HBase tests in GeoWave does not support running on mock instances like Accumulo.\n" + "Please provide a live running instance of zookeeper as zookeeperUrl property.");
					/*
					 * TODO #406 Currently HBase does tests does not support
					 * running on mock instances like Accumulo. Need to
					 * experiment with <code> HBaseTestingUtility </code> and
					 * check if it suits our purpose. // TEMP_DIR =
					 * Files.createTempDir(); if (!TEMP_DIR.exists()) { if
					 * (!TEMP_DIR.mkdirs()) { throw new IOException(
					 * "Could not create temporary directory"); } }
					 * TEMP_DIR.deleteOnExit(); utilty = new
					 * HBaseTestingUtility(); utilty.startMiniCluster(2); if
					 * (SystemUtils.IS_OS_WINDOWS) {
					 * LOGGER.error("Windows installation is not yet supported!!!"
					 * ); } zookeeper =
					 * utilty.getZooKeeperWatcher().getBaseZNode();
					 * hbaseInstance = utilty.getMiniHBaseCluster();
					 */
				}
				try {
					operations = new BasicHBaseOperations(
							zookeeper,
							TEST_NAMESPACE);
				}
				catch (IOException e) {
					LOGGER.warn(
							"Unable to connect to HBase",
							e);
					Assert.fail("Could not connect to HBase instance: '" + e.getLocalizedMessage() + "'");
				}
			}
		}
	}

	@SuppressFBWarnings(value = {
		"SWL_SLEEP_WITH_LOCK_HELD"
	}, justification = "Sleep in lock while waiting for external resources")
	@AfterClass
	public static void cleanup() {
		synchronized (MUTEX) {
			if (!DEFER_CLEANUP.get()) {

				if (operations == null) {
					Assert.fail("Invalid state <null> for hbase operations during CLEANUP phase");
				}
				try {
					operations.deleteAll();
				}
				catch (IOException ex) {
					LOGGER.error(
							"Unable to clear hbase namespace",
							ex);
					Assert.fail("Index not deleted successfully");
				}

				operations = null;
				zookeeper = null;

				if (TEMP_DIR != null) {
					try {
						Thread.sleep(1000);
						FileUtils.deleteDirectory(TEMP_DIR);
						TEMP_DIR = null;
					}
					catch (final IOException | InterruptedException e) {
						LOGGER.warn(
								"Unable to delete mini hbase temporary directory",
								e);
					}
				}
			}
		}
	}

	public BasicHBaseOperations getOperations() {
		return operations;
	}

	@Override
	protected void testLocalIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localhbaseingest -f geotools-vector -b " + ingestFilePath + " -z " + zookeeper + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		GeoWaveMain.main(args);
		verifyStats();
	}

	private void verifyStats() {
		GeoWaveMain.main(new String[] {
			"-hbasestatsdump",
			"-z",
			zookeeper,
			"-n",
			TEST_NAMESPACE
		});
	}

}
