package mil.nga.giat.geowave.examples.hbase;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;

public class HBaseMiniCluster
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMiniCluster.class);
	private static final String HBASE_PROPS_FILE = "hbase.properties";

	protected String zookeeper;
	private HbaseLocalCluster hbaseLocalCluster;
	private ZookeeperLocalCluster zookeeperLocalCluster;

	public static void main(
			final String[] args )
			throws Exception {
		final boolean interactive = (System.getProperty("interactive") != null) ? Boolean.parseBoolean(System
				.getProperty("interactive")) : true;

		final HBaseMiniCluster hbase = new HBaseMiniCluster();

		System.out.println("starting up HBase ...");
		Thread.sleep(3000);

		if (interactive) {
			System.out.println("hit Enter to shutdown ..");
			System.in.read();
			System.out.println("Shutting down!");
			hbase.tearDown();
		}
		else {
			Runtime.getRuntime().addShutdownHook(
					new Thread() {
						@Override
						public void run() {
							try {
								hbase.tearDown();
							}
							catch (final Exception e) {
								LOGGER.error(
										"Error shutting down hbase.",
										e);
							}
							System.out.println("Shutting down!");
						}
					});

			while (true) {
				Thread.sleep(TimeUnit.MILLISECONDS.convert(
						Long.MAX_VALUE,
						TimeUnit.DAYS));
			}
		}
	}

	public HBaseMiniCluster() {
		org.apache.log4j.Logger.getRootLogger().setLevel(
				org.apache.log4j.Level.DEBUG);
		LOGGER.debug("HBASE TEST SETUP!");

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

		if (System.getProperty(
				"os.name").startsWith(
				"Windows")) {
			System.setProperty(
					"HADOOP_HOME",
					System.getenv().get(
							"HADOOP_HOME"));
		}

		try {
			zookeeperLocalCluster = new ZookeeperLocalCluster.Builder().setPort(
					Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY))).setTempDir(
					propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY)).setZookeeperConnectionString(
					propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY)).build();
			zookeeperLocalCluster.start();
		}
		catch (final Exception e) {
			LOGGER.error("Exception starting zookeeperLocalCluster: " + e);
		}

		zookeeper = zookeeperLocalCluster.getZookeeperConnectionString();

		LOGGER.debug("Using local zookeeper URL: " + zookeeper);

		try {
			Configuration conf = new Configuration();
			conf.set(
					"hbase.online.schema.update.enable",
					"true");

			// Set list of coprocessors here (one for test, so far)
			// conf.set(
			// CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
			// AggregationEndpoint.class.getName());

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
			LOGGER.error("Exception starting hbaseLocalCluster: " + e);
		}
	}

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
}
