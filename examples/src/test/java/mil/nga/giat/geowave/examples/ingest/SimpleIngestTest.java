package mil.nga.giat.geowave.examples.ingest;

import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class SimpleIngestTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleIngestTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();
	final GeometryFactory factory = new GeometryFactory();
	final String AccumuloUser = "root";
	final PasswordToken AccumuloPass = new PasswordToken(
			new byte[0]);
	AccumuloOperations accumuloOperations;
	AccumuloIndexStore indexStore;
	AccumuloAdapterStore adapterStore;
	AccumuloDataStatisticsStore statsStore;
	AccumuloDataStore mockDataStore;

	@Before
	public void setUp() {
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					AccumuloUser,
					AccumuloPass);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}
		accumuloOperations = new BasicAccumuloOperations(
				mockConnector);

		indexStore = new AccumuloIndexStore(
				accumuloOperations);

		adapterStore = new AccumuloAdapterStore(
				accumuloOperations);

		statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				statsStore,
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations),
				new AccumuloAdapterIndexMappingStore(
						accumuloOperations),
				accumuloOperations,
				accumuloOptions);

		accumuloOptions.setCreateTable(true);
		accumuloOptions.setUseAltIndex(true);
		accumuloOptions.setPersistDataStatistics(true);
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
		CloseableIterator itr = ds.query(
				new QueryOptions(),
				new BasicQuery(
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

}
