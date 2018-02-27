package mil.nga.giat.geowave.test.basic;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

import org.geotools.referencing.CRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class GeowaveCustomCRSSpatialVectorIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeowaveCustomCRSSpatialVectorIT.class);
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-filter.shp";

	private static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-filter.shp";

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String CQL_DELETE_STR = "STATE = 'TX'";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("---------------------------------------------");
		LOGGER.warn("*                                           *");
		LOGGER.warn("*  RUNNING GeoWaveCustomCRSSpatialVectorIT  *");
		LOGGER.warn("*                                           *");
		LOGGER.warn("---------------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("---------------------------------------------");
		LOGGER.warn("*                                           *");
		LOGGER.warn("* FINISHED GeoWaveCustomCRSSpatialVectorIT  *");
		LOGGER.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
				+ "s elapsed.                            *");
		LOGGER.warn("*                                           *");
		LOGGER.warn("---------------------------------------------");
	}

	@Test
	public void testMultiThreadedIngestAndQuerySpatialPointsAndLines() {
		testIngestAndQuerySpatialPointsAndLines(1);
	}

	public void testIngestAndQuerySpatialPointsAndLines(
			final int nthreads ) {
		long mark = System.currentTimeMillis();

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TestUtils.CUSTOM_CRSCODE,
				HAIL_SHAPEFILE_FILE,
				"geotools-vector",
				nthreads);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + nthreads + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TestUtils.CUSTOM_CRSCODE,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				"geotools-vector",
				nthreads);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + nthreads + " thread(s).");
		try {
			CoordinateReferenceSystem crs = CRS.decode(TestUtils.CUSTOM_CRSCODE);
			mark = System.currentTimeMillis();

			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
					},
					TestUtils.createCustomCRSPrimaryIndex(),
					"bounding box constraint only",
					crs);

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("BBOX query duration = " + dur + " ms.");
			mark = System.currentTimeMillis();

			testQuery(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
					},
					TestUtils.createCustomCRSPrimaryIndex(),
					"polygon constraint only",
					crs);

			dur = (System.currentTimeMillis() - mark);
			LOGGER.debug("POLY query duration = " + dur + " ms.");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage()
					+ "'");
		}

		try {
			testStats(
					new URL[] {
						new File(
								HAIL_SHAPEFILE_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_SHAPEFILE_FILE).toURI().toURL()
					},
					TestUtils.createCustomCRSPrimaryIndex(),
					false,
					CRS.decode(TestUtils.CUSTOM_CRSCODE));
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box stats on spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDeleteCQL(
					CQL_DELETE_STR,
					TestUtils.createCustomCRSPrimaryIndex());
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDeleteSpatial(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					TestUtils.createCustomCRSPrimaryIndex());
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		TestUtils.deleteAll(dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
