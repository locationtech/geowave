package mil.nga.giat.geowave.test.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomWriter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveJavaSparkSQLIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveJavaSparkSQLIT.class);

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
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
	public void testCreateDataFrame() {
		// Set up Spark
		SparkSession spark = SparkSession.builder().master(
				"local[*]").appName(
				"JavaSparkSqlIT").getOrCreate();

		JavaSparkContext context = new JavaSparkContext(
				spark.sparkContext());

		// ingest test points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		try {
			// Load RDD from datastore, no filters
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> javaRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore);

			long count = javaRdd.count();
			LOGGER.warn("DataStore loaded into RDD with " + count + " features.");

			// Create a DataFrame from the RDD
			SimpleFeatureDataFrame sfDataFrame = new SimpleFeatureDataFrame(
					spark);

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

			Dataset<Row> results = spark.sql("SELECT * FROM features WHERE geomContains('" + bbox + "', geom)");
			long containsCount = results.count();
			LOGGER.warn("Got " + containsCount + " for geomContains test");

			results = spark.sql("SELECT * FROM features WHERE geomWithin(geom, '" + bbox + "')");
			long withinCount = results.count();
			LOGGER.warn("Got " + withinCount + " for geomWithin test");

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
			Row result = spark.sql(
					"SELECT geomIntersects('" + line1 + "', '" + line2 + "')").head();

			boolean intersect = result.getBoolean(0);
			LOGGER.warn("geomIntersects returned " + intersect);

			Assert.assertTrue(
					"Lines should intersect",
					intersect);

			result = spark.sql(
					"SELECT geomDisjoint('" + line1 + "', '" + line2 + "')").head();

			boolean disjoint = result.getBoolean(0);
			LOGGER.warn("geomDisjoint returned " + disjoint);

			Assert.assertFalse(
					"Lines should not be disjoint",
					disjoint);

		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			spark.close();
			context.close();
			Assert.fail("Error occurred while testing a bounding box query of spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		// Clean up
		TestUtils.deleteAll(dataStore);

		spark.close();
		context.close();
	}

	// @Test
	public void testSpatialJoin() {
		// Set up Spark
		SparkSession spark = SparkSession.builder().master(
				"local[*]").appName(
				"JavaSparkSqlIT").getOrCreate();

		JavaSparkContext context = new JavaSparkContext(
				spark.sparkContext());

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
					context.sc(),
					dataStore,
					leftBoxQuery);

			// Create a DataFrame from the Left RDD
			SimpleFeatureDataFrame leftDataFrame = new SimpleFeatureDataFrame(
					spark);

			if (!leftDataFrame.init(
					dataStore,
					null)) {
				Assert.fail("Failed to initialize dataframe");
			}

			Dataset<Row> dfLeft = leftDataFrame.getDataFrame(leftRdd);

			dfLeft.createOrReplaceTempView("left");

			Dataset<Row> leftDistinct = spark.sql("SELECT distinct geom FROM left");

			long leftCount = leftDistinct.count();
			LOGGER.warn("Left dataframe loaded with " + leftCount + " unique points.");

			// Load second RDD using spatial query (bbox) for 1/2-deg overlap
			String rightBboxStr = "POLYGON ((-93.5 34, -92.5 34, -92.5 35, -93.5 35, -93.5 34))";
			Geometry rightBox = wktReader.read(rightBboxStr);
			SpatialQuery rightBoxQuery = new SpatialQuery(
					rightBox);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRdd = GeoWaveRDD.rddForSimpleFeatures(
					context.sc(),
					dataStore,
					rightBoxQuery);

			// Create a DataFrame from the Left RDD
			SimpleFeatureDataFrame rightDataFrame = new SimpleFeatureDataFrame(
					spark);

			if (!rightDataFrame.init(
					dataStore,
					null)) {
				Assert.fail("Failed to initialize dataframe");
			}

			Dataset<Row> dfRight = rightDataFrame.getDataFrame(rightRdd);

			dfRight.createOrReplaceTempView("right");

			Dataset<Row> rightDistinct = spark.sql("SELECT distinct geom FROM right");

			long rightCount = rightDistinct.count();
			LOGGER.warn("Right dataframe loaded with " + rightCount + " unique points.");

			// Do a spatial join to find the overlap
			Dataset<Row> results = spark
					.sql("SELECT distinct left.geom FROM left INNER JOIN right ON geomIntersects(left.geom, right.geom)");

			long overlapCount = results.count();
			LOGGER.warn("Got " + overlapCount + " for spatial join intersection test");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			spark.close();
			context.close();
			Assert.fail("Error occurred while testing a bounding box query of spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		// Clean up
		TestUtils.deleteAll(dataStore);

		spark.close();
		context.close();
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
