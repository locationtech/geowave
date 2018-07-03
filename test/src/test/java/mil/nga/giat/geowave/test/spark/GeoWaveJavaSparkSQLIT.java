package mil.nga.giat.geowave.test.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDDLoader;
import mil.nga.giat.geowave.analytic.spark.RDDOptions;
import mil.nga.giat.geowave.analytic.spark.sparksql.SqlQueryRunner;
import mil.nga.giat.geowave.analytic.spark.sparksql.SqlResultsWriter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.SPARK
})
public class GeoWaveJavaSparkSQLIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveJavaSparkSQLIT.class);

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static Stopwatch stopwatch = new Stopwatch();

	@BeforeClass
	public static void reportTestStart() {
		stopwatch.reset();
		stopwatch.start();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveJavaSparkSQLIT        *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		stopwatch.stop();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveJavaSparkSQLIT        *");
		LOGGER.warn("*         " + stopwatch.getTimeString() + " elapsed.             *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testCreateDataFrame()
			throws Exception {
		// Set up Spark
		SparkContext context = SparkTestEnvironment.getInstance().getDefaultContext();
		SparkSession session = SparkTestEnvironment.getInstance().getDefaultSession();

		// ingest test points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		SqlQueryRunner queryRunner = new SqlQueryRunner();
		queryRunner.setSparkSession(session);

		try {
			// Load RDD from datastore, no filters
			GeoWaveRDD newRDD = GeoWaveRDDLoader.loadRDD(
					context,
					dataStore,
					new RDDOptions());
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = newRDD.getRawRDD();

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

			queryRunner.addInputStore(
					dataStore,
					null,
					"features");

			String bbox = "POLYGON ((-94 34, -93 34, -93 35, -94 35, -94 34))";

			queryRunner.setSql("SELECT * FROM features WHERE GeomContains('" + bbox + "', geom)");

			Dataset<Row> results = queryRunner.run();
			long containsCount = results.count();
			LOGGER.warn("Got " + containsCount + " for GeomContains test");

			queryRunner.setSql("SELECT * FROM features WHERE GeomWithin(geom, '" + bbox + "')");
			results = queryRunner.run();
			long withinCount = results.count();
			LOGGER.warn("Got " + withinCount + " for GeomWithin test");

			Assert.assertTrue(
					"Within and Contains counts should be equal",
					containsCount == withinCount);

			// Test the output writer
			SqlResultsWriter sqlResultsWriter = new SqlResultsWriter(
					results,
					dataStore);

			sqlResultsWriter.writeResults("sqltest");

			queryRunner.removeAllStores();

			// Test other spatial UDFs
			String line1 = "LINESTRING(0 0, 10 10)";
			String line2 = "LINESTRING(0 10, 10 0)";
			queryRunner.setSql("SELECT GeomIntersects('" + line1 + "', '" + line2 + "')");
			Row result = queryRunner.run().head();

			boolean intersect = result.getBoolean(0);
			LOGGER.warn("GeomIntersects returned " + intersect);

			Assert.assertTrue(
					"Lines should intersect",
					intersect);

			queryRunner.setSql("SELECT GeomDisjoint('" + line1 + "', '" + line2 + "')");
			result = queryRunner.run().head();

			boolean disjoint = result.getBoolean(0);
			LOGGER.warn("GeomDisjoint returned " + disjoint);

			Assert.assertFalse(
					"Lines should not be disjoint",
					disjoint);

		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box query of spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		// Clean up
		TestUtils.deleteAll(dataStore);
	}

	@Test
	public void testSpatialJoin()
			throws Exception {

		// Set up Spark
		SparkSession session = SparkTestEnvironment.getInstance().getDefaultSession();

		SqlQueryRunner queryRunner = new SqlQueryRunner();
		queryRunner.setSparkSession(session);

		// ingest test points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		try {
			// Run a valid sql query that should do a optimized join
			queryRunner.addInputStore(
					dataStore,
					new ByteArrayId(
							"hail"),
					"hail");
			queryRunner.addInputStore(
					dataStore,
					new ByteArrayId(
							"tornado_tracks"),
					"tornado");
			queryRunner.setSql("select hail.* from hail, tornado where GeomIntersects(hail.geom, tornado.geom)");
			Dataset<Row> results = queryRunner.run();
			LOGGER.warn("Indexed intersect from sql returns: " + results.count() + " results.");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while attempting optimized join from sql query runner: '"
					+ e.getLocalizedMessage() + "'");
		}

		// Clean up
		TestUtils.deleteAll(dataStore);
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
