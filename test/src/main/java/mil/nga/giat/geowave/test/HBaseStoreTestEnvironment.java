package mil.nga.giat.geowave.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;

public class HBaseStoreTestEnvironment implements
		StoreTestEnvironment
{
	private static HBaseStoreTestEnvironment singletonInstance = null;

	public static synchronized HBaseStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new HBaseStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = Logger.getLogger(HBaseStoreTestEnvironment.class);
	private static final String HBASE_PROPS_FILE = "hbase.properties";
	protected String zookeeper;
	private HbaseLocalCluster hbaseLocalCluster;
	private ZookeeperLocalCluster zookeeperLocalCluster;

	private HBaseStoreTestEnvironment() {}

	@Override
	public DataStorePluginOptions getDataStoreOptions(
			final String namespace ) {
		final DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		final HBaseRequiredOptions opts = new HBaseRequiredOptions();
		opts.setGeowaveNamespace(namespace);
		opts.setZookeeper(zookeeper);
		pluginOptions.selectPlugin(new HBaseDataStoreFactory().getName());
		pluginOptions.setFactoryOptions(opts);
		return pluginOptions;
	}

	@Override
	public void setup() {
		LOGGER.setLevel(Level.DEBUG);
		LOGGER.debug("HBASE TEST SETUP!");

		if (!TestUtils.isSet(zookeeper)) {
			zookeeper = System.getProperty("zookeeperUrl");

			if (!TestUtils.isSet(zookeeper)) {

				PropertyParser propertyParser = null;

				try {
					propertyParser = new PropertyParser(
							HBASE_PROPS_FILE);
					propertyParser.parsePropsFile();
				}
				catch (final IOException e) {
					LOGGER.error("Unable to load property file: {}" + HBASE_PROPS_FILE);
				}

				if (System.getProperty(
						"os.name").startsWith(
						"Windows")) {
					System.setProperty(
							"HADOOP_HOME",
							System.getenv().get(
									"HADOOP_HOME"));
				}

				try {
					zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
							.setPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
							.setTempDir(
									propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
							.setZookeeperConnectionString(
									propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
							.build();
					zookeeperLocalCluster.start();
				}
				catch (final Exception e) {
					LOGGER.error("Exception starting zookeeperLocalCluster: " + e);
					e.printStackTrace();
					Assert.fail();
				}

				zookeeper = zookeeperLocalCluster.getZookeeperConnectionString();

				LOGGER.debug("Using local zookeeper URL: " + zookeeper);

				try {
					Configuration conf = new Configuration();
					conf.set(
							"hbase.online.schema.update.enable",
							"true");
					hbaseLocalCluster = new HbaseLocalCluster.Builder()
							.setHbaseMasterPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
							.setHbaseMasterInfoPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
							.setNumRegionServers(
									Integer.parseInt(propertyParser
											.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
							.setHbaseRootDir(
									propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
							.setZookeeperPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
							.setZookeeperConnectionString(
									propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
							.setZookeeperZnodeParent(
									propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
							.setHbaseWalReplicationEnabled(
									Boolean.parseBoolean(propertyParser
											.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
							.setHbaseConfiguration(
									conf)
							.build();
					hbaseLocalCluster.start();
				}
				catch (final Exception e) {
					LOGGER.error("Exception starting hbaseLocalCluster: " + e);
					e.printStackTrace();
					Assert.fail();
				}
			}
			else {
				LOGGER.debug("Using system zookeeper URL: " + zookeeper);
			}
		}
		else {
			LOGGER.debug("Using system zookeeper URL: " + zookeeper);
		}
	}

	@Override
	public void tearDown() {
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

		zookeeper = null;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		// TODO: create zookeeper test environment as dependency
		return new TestEnvironment[] {};
	}
}
