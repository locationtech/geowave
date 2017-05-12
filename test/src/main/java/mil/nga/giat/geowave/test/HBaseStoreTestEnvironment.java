package mil.nga.giat.geowave.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class HBaseStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new HBaseDataStoreFactory();
	private static HBaseStoreTestEnvironment singletonInstance = null;

	public static synchronized HBaseStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new HBaseStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseStoreTestEnvironment.class);
	public static final String HBASE_PROPS_FILE = "hbase.properties";
	protected String zookeeper;
	private HbaseLocalCluster hbaseLocalCluster;

	private HBaseStoreTestEnvironment() {}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		((HBaseRequiredOptions) options).setZookeeper(zookeeper);
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	public void setup() {
		PropertyParser propertyParser = null;

		try {
			propertyParser = new PropertyParser(
					HBASE_PROPS_FILE);
			propertyParser.parsePropsFile();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to load property file: {}" + HBASE_PROPS_FILE,
					e);
		}

		if (!TestUtils.isSet(zookeeper)) {
			zookeeper = System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME);

			if (!TestUtils.isSet(zookeeper)) {
				zookeeper = ZookeeperTestEnvironment.getInstance().getZookeeper();
				LOGGER.debug("Using local zookeeper URL: " + zookeeper);
			}
		}

		if ((hbaseLocalCluster == null)
				&& !TestUtils.isSet(System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME))) {
			try {
				final Configuration conf = new Configuration();
				conf.set(
						"hbase.online.schema.update.enable",
						"true");
				hbaseLocalCluster = new HbaseLocalCluster.Builder()
						.setHbaseMasterPort(
								Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
						.setHbaseMasterInfoPort(
								Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
						.setNumRegionServers(
								Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
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
				LOGGER.error(
						"Exception starting hbaseLocalCluster",
						e);
				Assert.fail();
			}
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
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.HBASE;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}
