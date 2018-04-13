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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.util.Stopwatch;

import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.analytic.spark.sparksql.SimpleFeatureDataFrame;
import mil.nga.giat.geowave.analytic.spark.sparksql.SqlResultsWriter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
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
		GeoWaveStoreType.CASSANDRA
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

		try {
			// Load RDD from datastore, no filters
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context,
					dataStore);

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

			// Create a DataFrame from the RDD
			SimpleFeatureDataFrame sfDataFrame = new SimpleFeatureDataFrame(
					session);

			if (!sfDataFrame.init(
					dataStore,
					null)) {
				Assert.fail("Failed to initialize dataframe");
			}

			LOGGER.warn(sfDataFrame.getSchema().json());

			Dataset<Row> df = sfDataFrame.getDataFrame(javaRdd);
			df.show(10);

			df.createOrReplaceTempView("features");

			String bbox = "POLYGON ((-94 34, -93 34, -93 35, -94 35, -94 34))";

			Dataset<Row> results = session.sql("SELECT * FROM features WHERE GeomContains('" + bbox + "', geom)");
			long containsCount = results.count();
			LOGGER.warn("Got " + containsCount + " for GeomContains test");

			results = session.sql("SELECT * FROM features WHERE GeomWithin(geom, '" + bbox + "')");
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

			// Test other spatial UDFs
			String line1 = "LINESTRING(0 0, 10 10)";
			String line2 = "LINESTRING(0 10, 10 0)";
			Row result = session.sql(
					"SELECT GeomIntersects('" + line1 + "', '" + line2 + "')").head();

			boolean intersect = result.getBoolean(0);
			LOGGER.warn("GeomIntersects returned " + intersect);

			Assert.assertTrue(
					"Lines should intersect",
					intersect);

			result = session.sql(
					"SELECT GeomDisjoint('" + line1 + "', '" + line2 + "')").head();

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

	// @Test
	public void testSpatialJoin()
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

		try {
			WKTReader wktReader = new WKTReader();

			// Load first RDD using spatial query (bbox)
			String leftBboxStr = "POLYGON ((-94 34, -93 34, -93 35, -94 35, -94 34))";
			Geometry leftBox = wktReader.read(leftBboxStr);
			SpatialQuery leftBoxQuery = new SpatialQuery(
					leftBox);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRdd = GeoWaveRDD.rddForSimpleFeatures(
					context,
					dataStore,
					leftBoxQuery);

			// Create a DataFrame from the Left RDD
			SimpleFeatureDataFrame leftDataFrame = new SimpleFeatureDataFrame(
					session);

			if (!leftDataFrame.init(
					dataStore,
					null)) {
				Assert.fail("Failed to initialize dataframe");
			}

			Dataset<Row> dfLeft = leftDataFrame.getDataFrame(leftRdd);

			dfLeft.createOrReplaceTempView("left");

			Dataset<Row> leftDistinct = session.sql("SELECT distinct geom FROM left");

			long leftCount = leftDistinct.count();
			LOGGER.warn("Left dataframe loaded with " + leftCount + " unique points.");

			// Load second RDD using spatial query (bbox) for 1/2-deg overlap
			String rightBboxStr = "POLYGON ((-93.5 34, -92.5 34, -92.5 35, -93.5 35, -93.5 34))";
			Geometry rightBox = wktReader.read(rightBboxStr);
			SpatialQuery rightBoxQuery = new SpatialQuery(
					rightBox);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRdd = GeoWaveRDD.rddForSimpleFeatures(
					context,
					dataStore,
					rightBoxQuery);

			// Create a DataFrame from the Left RDD
			SimpleFeatureDataFrame rightDataFrame = new SimpleFeatureDataFrame(
					session);

			if (!rightDataFrame.init(
					dataStore,
					null)) {
				Assert.fail("Failed to initialize dataframe");
			}

			Dataset<Row> dfRight = rightDataFrame.getDataFrame(rightRdd);

			dfRight.createOrReplaceTempView("right");

			Dataset<Row> rightDistinct = session.sql("SELECT distinct geom FROM right");

			long rightCount = rightDistinct.count();
			LOGGER.warn("Right dataframe loaded with " + rightCount + " unique points.");

			// Do a spatial join to find the overlap
			Dataset<Row> results = session
					.sql("SELECT distinct left.geom FROM left INNER JOIN right ON geomIntersects(left.geom, right.geom)");

			long overlapCount = results.count();
			LOGGER.warn("Got " + overlapCount + " for spatial join intersection test");
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

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
