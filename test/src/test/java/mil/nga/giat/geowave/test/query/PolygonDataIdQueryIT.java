package mil.nga.giat.geowave.test.query;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class PolygonDataIdQueryIT
{
	private static final Logger LOGGER = LoggerFactory.getLogger(PolygonDataIdQueryIT.class);
	private static SimpleFeatureType simpleFeatureType;
	private static FeatureDataAdapter dataAdapter;
	private static final String GEOMETRY_ATTRIBUTE = "geometry";
	private static final String DATA_ID = "dataId";
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;

	@Test
	public void testPolygonDataIdQueryResults() {
		final CloseableIterator<SimpleFeature> matches = dataStore.createDataStore().query(
				new QueryOptions(
						dataAdapter,
						TestUtils.DEFAULT_SPATIAL_INDEX),
				new DataIdQuery(
						dataAdapter.getAdapterId(),
						new ByteArrayId(
								StringUtils.stringToBinary(DATA_ID))));
		int numResults = 0;
		while (matches.hasNext()) {
			matches.next();
			numResults++;
		}
		Assert.assertTrue(
				"Expected 1 result, but returned " + numResults,
				numResults == 1);
	}

	@BeforeClass
	public static void setupData()
			throws IOException {
		simpleFeatureType = getSimpleFeatureType();
		dataAdapter = new FeatureDataAdapter(
				simpleFeatureType);

		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING PolygonDataIdQueryIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED PolygonDataIdQueryIT    *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Before
	public void ingestSampleData()
			throws IOException {
		try (@SuppressWarnings("unchecked")
		IndexWriter writer = dataStore.createDataStore().createWriter(
				dataAdapter,
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			writer.write(buildSimpleFeature(
					DATA_ID,
					GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
						new Coordinate(
								1.0249,
								1.0319),
						new Coordinate(
								1.0261,
								1.0319),
						new Coordinate(
								1.0261,
								1.0323),
						new Coordinate(
								1.0249,
								1.0319)
					})));
		}
	}

	@After
	public void deleteSampleData()
			throws IOException {

		LOGGER.info("Deleting canned data...");
		TestUtils.deleteAll(dataStore);
		LOGGER.info("Delete complete.");
	}

	private static SimpleFeatureType getSimpleFeatureType() {
		SimpleFeatureType type = null;
		try {
			type = DataUtilities.createType(
					"data",
					GEOMETRY_ATTRIBUTE + ":Geometry");
		}
		catch (final SchemaException e) {
			LOGGER.error(
					"Unable to create SimpleFeatureType",
					e);
		}
		return type;
	}

	private static SimpleFeature buildSimpleFeature(
			final String dataId,
			final Geometry geo ) {
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				simpleFeatureType);
		builder.set(
				GEOMETRY_ATTRIBUTE,
				geo);
		return builder.buildFeature(dataId);
	}
}
