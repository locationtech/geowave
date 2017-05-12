package mil.nga.giat.geowave.examples.ingest;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

public class SimpleHBaseIngestTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleHBaseIngestTest.class);

	private final static HBaseOptions hbaseOptions = new HBaseOptions();
	private static HBaseIndexStore indexStore;
	private static HBaseAdapterStore adapterStore;
	private static HBaseDataStatisticsStore statsStore;
	private static HBaseAdapterIndexMappingStore mockIndexMappingStore;
	private static HBaseSecondaryIndexDataStore secondaryIndexStore;
	private static HBaseDataStore mockDataStore;

	private static String zookeeper;
	private static HbaseLocalCluster hbaseLocalCluster;
	private static ZookeeperLocalCluster zookeeperLocalCluster;
	private static final String HBASE_PROPS_FILE = "hbase.properties";
	private static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
	private static BasicHBaseOperations hbaseOperations;

	@BeforeClass
	public static void setUp() {

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
			zookeeperLocalCluster = new ZookeeperLocalCluster.Builder().setPort(
					Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY))).setTempDir(
					propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY)).setZookeeperConnectionString(
					propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY)).build();
			zookeeperLocalCluster.start();
		}
		catch (final Exception e) {
			LOGGER.error("Exception starting zookeeperLocalCluster: " + e);
			e.printStackTrace();
			Assert.fail();
		}

		zookeeper = zookeeperLocalCluster.getZookeeperConnectionString();

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
			e.printStackTrace();
			Assert.fail();
		}

		try {
			hbaseOperations = new BasicHBaseOperations(
					zookeeperLocalCluster.getZookeeperConnectionString(),
					TEST_NAMESPACE);
		}
		catch (final IOException e) {
			LOGGER.error("Exception connecting to hbaseLocalCluster: " + e);
			Assert.fail();
		}

		indexStore = new HBaseIndexStore(
				hbaseOperations);

		adapterStore = new HBaseAdapterStore(
				hbaseOperations);

		statsStore = new HBaseDataStatisticsStore(
				hbaseOperations);

		secondaryIndexStore = new HBaseSecondaryIndexDataStore(
				hbaseOperations);

		mockIndexMappingStore = new HBaseAdapterIndexMappingStore(
				hbaseOperations);

		mockDataStore = new HBaseDataStore(
				indexStore,
				adapterStore,
				statsStore,
				mockIndexMappingStore,
				secondaryIndexStore,
				hbaseOperations,
				hbaseOptions);

		hbaseOptions.setCreateTable(true);
		hbaseOptions.setUseAltIndex(true);
		hbaseOptions.setPersistDataStatistics(true);
	}

	@SuppressFBWarnings(value = {
		"SWL_SLEEP_WITH_LOCK_HELD"
	}, justification = "Sleep in lock while waiting for external resources")
	@AfterClass
	public static void cleanup() {

		try {
			hbaseOperations.deleteAll();
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Unable to clear hbase namespace",
					ex);
			Assert.fail("Index not deleted successfully");
		}
		catch (final NullPointerException npe) {
			Assert.fail("Invalid state <null> for hbase operations during CLEANUP phase");
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

		hbaseOperations = null;
		zookeeper = null;
	}

	@Test
	public void TestIngestIndexWriter() {
		final SimpleIngestIndexWriter si = new SimpleIngestIndexWriter();
		si.generateGrid(mockDataStore);
		validate(mockDataStore);
		mockDataStore.delete(
				new QueryOptions(),
				null);
	}

	@Test
	public void TestIngestProducerConsumer() {
		final SimpleIngestProducerConsumer si = new SimpleIngestProducerConsumer();
		si.generateGrid(mockDataStore);
		validate(mockDataStore);
		mockDataStore.delete(
				new QueryOptions(),
				null);
	}

	protected static Set<Point> getCalcedPointSet() {
		final Set<Point> calcPoints = new TreeSet<Point>();
		for (int longitude = -180; longitude <= 180; longitude += 5) {
			for (int latitude = -90; latitude <= 90; latitude += 5) {
				final Point p = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						longitude,
						latitude));
				calcPoints.add(p);
			}
		}
		return calcPoints;
	}

	protected static Set<Point> getStoredPointSet(
			final DataStore ds ) {
		final CloseableIterator itr = ds.query(
				new QueryOptions(),
				new BasicQuery(
						new BasicQuery.Constraints()));
		final Set<Point> readPoints = new TreeSet<Point>();
		while (itr.hasNext()) {
			final Object n = itr.next();
			if (n instanceof SimpleFeature) {
				final SimpleFeature gridCell = (SimpleFeature) n;
				final Point p = (Point) gridCell.getDefaultGeometry();
				readPoints.add(p);
			}
		}
		return readPoints;
	}

	protected static void validate(
			final DataStore ds ) {
		final Set<Point> readPoints = getStoredPointSet(ds);
		final Set<Point> calcPoints = getCalcedPointSet();

		Assert.assertTrue(readPoints.equals(calcPoints));
	}

}
