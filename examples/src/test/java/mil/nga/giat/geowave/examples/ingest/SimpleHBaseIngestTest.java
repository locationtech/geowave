package mil.nga.giat.geowave.examples.ingest;

import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.datastore.hbase.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class SimpleHBaseIngestTest
{
	private final static Logger LOGGER = Logger.getLogger(SimpleHBaseIngestTest.class);

	final HBaseOptions hbaseOptions = new HBaseOptions();
	final GeometryFactory factory = new GeometryFactory();
	BasicHBaseOperations hbaseOperations;
	HBaseIndexStore indexStore;
	HBaseAdapterStore adapterStore;
	HBaseDataStatisticsStore statsStore;
	HBaseDataStore mockDataStore;
	private final HBaseTestingUtility testUtil = new HBaseTestingUtility();

	@Before
	public void setUp() {
		Connection connection = null;
		testUtil.getConfiguration().addResource(
				"hbase-site-local.xml");
		testUtil.getConfiguration().reloadConfiguration();
		// start mini hbase cluster
		try {
			testUtil.startMiniCluster(1);
			connection = testUtil.getConnection();
		}
		catch (Exception e) {
			LOGGER.error(
					"Failed to create HBase MiniCluster",
					e);
		}

		hbaseOperations = new BasicHBaseOperations(
				connection);

		indexStore = new HBaseIndexStore(
				hbaseOperations);

		adapterStore = new HBaseAdapterStore(
				hbaseOperations);

		statsStore = new HBaseDataStatisticsStore(
				hbaseOperations);

		mockDataStore = new HBaseDataStore(
				indexStore,
				adapterStore,
				statsStore,
				hbaseOperations,
				hbaseOptions);

		hbaseOptions.setCreateTable(true);
		hbaseOptions.setUseAltIndex(true);
		hbaseOptions.setPersistDataStatistics(true);
	}

	protected static Set<Point> getCalcedPointSet() {
		Set<Point> calcPoints = new TreeSet<Point>();
		for (int longitude = -180; longitude <= 180; longitude += 5) {
			for (int latitude = -90; latitude <= 90; latitude += 5) {
				Point p = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						longitude,
						latitude));
				calcPoints.add(p);
			}
		}
		return calcPoints;
	}

	protected static Set<Point> getStoredPointSet(
			DataStore ds ) {
		CloseableIterator itr = ds.query(new BasicQuery(
				new BasicQuery.Constraints()));
		Set<Point> readPoints = new TreeSet<Point>();
		while (itr.hasNext()) {
			Object n = itr.next();
			if (n instanceof SimpleFeature) {
				SimpleFeature gridCell = (SimpleFeature) n;
				Point p = (Point) gridCell.getDefaultGeometry();
				readPoints.add(p);
			}
		}
		return readPoints;
	}

	protected static void validate(
			DataStore ds ) {
		Set<Point> readPoints = getStoredPointSet(ds);
		Set<Point> calcPoints = getCalcedPointSet();

		Assert.assertTrue(readPoints.equals(calcPoints));
	}

	@Test
	public void TestIngest() {
		final SimpleIngest si = new SimpleIngest();
		si.generateGrid(mockDataStore);
		validate(mockDataStore);
	}

}
