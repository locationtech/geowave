package mil.nga.giat.geowave.test.spark;

import java.io.File;
import java.net.URL;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.datastore.hbase.cli.HBaseMiniCluster;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveSparkSpatialJoinIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);
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
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveSparkSpatialJoinIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveSparkSpatialJoinIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}
	
	@Before
	public static void standupHBase() {
		try {
			HBaseMiniCluster.main(null);
		}
		catch (Exception e) {
			LOGGER.error("Unable to standup HBase for test.");
			e.printStackTrace();
		}
		
	}

	public void testSpatialJoin(
			final int nthreads ) {
		long mark = System.currentTimeMillis();

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());

		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				nthreads);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + nthreads + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				nthreads);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + nthreads + " thread(s).");

		

		TestUtils.deleteAll(dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}