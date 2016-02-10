/**
 *
 */
package mil.nga.giat.geowave.test;

import java.io.IOException;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy
 *
 */
public class GeoWaveHBaseTestEnvironment extends
		GeoWaveTestEnvironment
{

	private final static Logger LOGGER = Logger.getLogger(
			GeoWaveHBaseTestEnvironment.class);
	private static final String HBASE_PROPS_FILE = "hbase.properties";
	protected static BasicHBaseOperations operations;
	protected static String zookeeper;

	private static HbaseLocalCluster hbaseLocalCluster;
	private static ZookeeperLocalCluster zookeeperLocalCluster;

	@BeforeClass
	public static void setup()
			throws IOException {

		synchronized (MUTEX) {
			TimeZone.setDefault(
					TimeZone.getTimeZone(
							"GMT"));
			if (operations == null) {
				zookeeper = System.getProperty(
						"zookeeperUrl");
				if (!isSet(
						zookeeper)) {

					PropertyParser propertyParser = null;

					try {
						propertyParser = new PropertyParser(
								HBASE_PROPS_FILE);
						propertyParser.parsePropsFile();
					}
					catch (final IOException e) {
						LOGGER.error(
								"Unable to load property file: {}" + HBASE_PROPS_FILE);
					}

					System.setProperty("HADOOP_HOME", System.getenv().get("HADOOP_HOME"));

					try {
						zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
								.setPort(
										Integer.parseInt(
												propertyParser.getProperty(
														ConfigVars.ZOOKEEPER_PORT_KEY)))
								.setTempDir(
										propertyParser.getProperty(
												ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
								.setZookeeperConnectionString(
										propertyParser.getProperty(
												ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
								.build();
						zookeeperLocalCluster.start();
					}
					catch (final Exception e) {
						LOGGER.error("Exception starting zookeeperLocalCluster: " + e);
						e.printStackTrace();
						Assert.fail();
					}

					zookeeper = zookeeperLocalCluster.getZookeeperConnectionString();

					try {
						hbaseLocalCluster = new HbaseLocalCluster.Builder()
								.setHbaseMasterPort(
										Integer.parseInt(
												propertyParser.getProperty(
														ConfigVars.HBASE_MASTER_PORT_KEY)))
								.setHbaseMasterInfoPort(
										Integer.parseInt(
												propertyParser.getProperty(
														ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
								.setNumRegionServers(
										Integer.parseInt(
												propertyParser.getProperty(
														ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
								.setHbaseRootDir(
										propertyParser.getProperty(
												ConfigVars.HBASE_ROOT_DIR_KEY))
								.setZookeeperPort(
										Integer.parseInt(
												propertyParser.getProperty(
														ConfigVars.ZOOKEEPER_PORT_KEY)))
								.setZookeeperConnectionString(
										propertyParser.getProperty(
												ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
								.setZookeeperZnodeParent(
										propertyParser.getProperty(
												ConfigVars.HBASE_ZNODE_PARENT_KEY))
								.setHbaseWalReplicationEnabled(
										Boolean.parseBoolean(
												propertyParser.getProperty(
														ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
								.setHbaseConfiguration(
										new Configuration())
								.build();
						hbaseLocalCluster.start();
					}
					catch (final Exception e) {
						LOGGER.error("Exception starting hbaseLocalCluster: " + e);
						e.printStackTrace();
						Assert.fail();
					}

					operations = new BasicHBaseOperations(
							zookeeperLocalCluster.getZookeeperConnectionString(),
							TEST_NAMESPACE);

				}
				else {
					try {
						operations = new BasicHBaseOperations(
								zookeeper,
								TEST_NAMESPACE);
					}
					catch (final IOException e) {
						LOGGER.warn(
								"Unable to connect to HBase",
								e);
						Assert.fail(
								"Could not connect to HBase instance: '" + e.getLocalizedMessage() + "'");
					}
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
					Assert.fail(
							"Invalid state <null> for hbase operations during CLEANUP phase");
				}
				try {
					operations.deleteAll();
				}
				catch (final IOException ex) {
					LOGGER.error(
							"Unable to clear hbase namespace",
							ex);
					Assert.fail(
							"Index not deleted successfully");
				}

				try {
					hbaseLocalCluster.stop(true);
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to delete mini hbase temporary directory",
							e);
				}

				try {
					zookeeperLocalCluster.stop(true);
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to delete mini zookeeper temporary directory",
							e);
				}

				operations = null;
				zookeeper = null;
			}
		}
	}

	public BasicHBaseOperations getOperations() {
		return operations;
	}

	@Override
	protected void testLocalIngest(
			final DimensionalityType dimensionalityType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn(
				"Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localingest -datastore " + new HBaseDataStoreFactory().getName() + " -f geotools-vector -b " + ingestFilePath + " -" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY + " " + TEST_NAMESPACE + " -dim " + dimensionalityType.getDimensionalityArg() + " -" + BasicHBaseOperations.ZOOKEEPER_INSTANCES_NAME + " " + zookeeper,
				' ');
		GeoWaveMain.run(
				args);
		verifyStats();
	}

	private void verifyStats() {
		GeoWaveMain.run(new String[] {
			"-statsdump",
			"-" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
			TEST_NAMESPACE,
			"-datastore",
			new HBaseDataStoreFactory().getName(),
			"-" + BasicHBaseOperations.ZOOKEEPER_INSTANCES_NAME,
			zookeeper
		});
	}

}
