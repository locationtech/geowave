package mil.nga.giat.geowave.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;

public class ZookeeperTestEnvironment implements
		TestEnvironment
{

	private static ZookeeperTestEnvironment singletonInstance = null;

	public static synchronized ZookeeperTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ZookeeperTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(ZookeeperTestEnvironment.class);
	protected String zookeeper;

	private ZookeeperLocalCluster zookeeperLocalCluster;

	public static final String ZK_PROPERTY_NAME = "zookeeperUrl";

	private ZookeeperTestEnvironment() {}

	@Override
	public void setup()
			throws Exception {

		if (!TestUtils.isSet(zookeeper)) {
			zookeeper = System.getProperty(ZK_PROPERTY_NAME);

			if (!TestUtils.isSet(zookeeper)) {

				PropertyParser propertyParser = null;

				try {
					propertyParser = new PropertyParser(
							HBaseStoreTestEnvironment.HBASE_PROPS_FILE);
					propertyParser.parsePropsFile();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to load property file: {}" + HBaseStoreTestEnvironment.HBASE_PROPS_FILE,
							e);
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
					LOGGER.error(
							"Exception starting zookeeperLocalCluster: " + e,
							e);
					Assert.fail();
				}

				zookeeper = zookeeperLocalCluster.getZookeeperConnectionString();
			}
		}
	}

	@Override
	public void tearDown()
			throws Exception {
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

	public String getZookeeper() {
		return zookeeper;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}
