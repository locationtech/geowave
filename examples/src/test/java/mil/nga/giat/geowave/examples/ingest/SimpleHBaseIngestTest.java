package mil.nga.giat.geowave.examples.ingest;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class SimpleHBaseIngestTest
{
	private final static Logger LOGGER = Logger.getLogger(
			SimpleHBaseIngestTest.class);

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
			testUtil.startMiniCluster(
					1);
			connection = testUtil.getConnection();
		}
		catch (final Exception e) {
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

		hbaseOptions.setCreateTable(
				true);
		hbaseOptions.setUseAltIndex(
				true);
		hbaseOptions.setPersistDataStatistics(
				true);
	}

	protected static Set<Point> getCalcedPointSet() {
		final Set<Point> calcPoints = new TreeSet<Point>();
		for (int longitude = -180; longitude <= 180; longitude += 5) {
			for (int latitude = -90; latitude <= 90; latitude += 5) {
				final Point p = GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								longitude,
								latitude));
				calcPoints.add(
						p);
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
				readPoints.add(
						p);
			}
		}
		return readPoints;
	}

	protected static void validate(
			final DataStore ds ) {
		final Set<Point> readPoints = getStoredPointSet(
				ds);
		final Set<Point> calcPoints = getCalcedPointSet();

		Assert.assertTrue(
				readPoints.equals(
						calcPoints));
	}

}
