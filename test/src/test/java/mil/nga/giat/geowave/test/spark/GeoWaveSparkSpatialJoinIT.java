package mil.nga.giat.geowave.test.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import mil.nga.giat.geowave.analytic.spark.GeoWaveRDD;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.analytic.spark.sparksql.SimpleFeatureDataFrame;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunctionRegistry;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomIntersects;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import mil.nga.giat.geowave.analytic.spark.spatial.SpatialJoin;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveSparkSpatialJoinIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);
	
	@GeoWaveTestStore(value = {
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;
	private static SparkSession session;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		session = SparkSession
				.builder()
				.appName("SpatialJoinTest")
				.master("local")
				.config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
				.config("spark.kryo.registrator", "mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator")
				.getOrCreate();

		GeomFunctionRegistry.registerGeometryFunctions(session);
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

	@Test
	public void testHailTornadoIntersection() {
		
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());
		

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());
		long mark = System.currentTimeMillis();
		
		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				HAIL_SHAPEFILE_FILE,
				1);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + 1 + " thread(s).");

		mark = System.currentTimeMillis();

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (lines) duration = " + dur + " ms with " + 1 + " thread(s).");
		
		SpatialJoin join = new SpatialJoin();
		ByteArrayId hail_adapter = new ByteArrayId("hail");
		ByteArrayId tornado_adapter = new ByteArrayId("tornado_tracks");
		GeomIntersects predicate = new GeomIntersects();
		
		long tornadoIndexedCount = 0;
		long hailIndexedCount = 0;
		Dataset<Row> tornadoIndexedFrame = null;
		Dataset<Row> hailIndexedFrame = null;

		LOGGER.warn("------------ Running indexed spatial join. ----------");
		mark = System.currentTimeMillis();
		try {
			join.performJoin(sc.sc(), dataStore, hail_adapter, dataStore, tornado_adapter, predicate);

			hailIndexedCount = join.leftJoined.count();
			tornadoIndexedCount = join.rightJoined.count();
			
			SimpleFeatureDataFrame joinFrame = new SimpleFeatureDataFrame(session);
			
			joinFrame.init(dataStore, tornado_adapter);
			tornadoIndexedFrame = joinFrame.getDataFrame(join.rightJoined);
			

			joinFrame.init(dataStore, hail_adapter);
			hailIndexedFrame = joinFrame.getDataFrame(join.leftJoined);
		}
		catch (IOException e) {
			LOGGER.error("Could not perform join");
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			sc.close();
			Assert.fail();
		}
		long indexJoinDur = (System.currentTimeMillis() - mark);

		long tornadoBruteCount = 0;
		long hailBruteCount = 0;
		Dataset<Row> hailBruteResults = null;
		Dataset<Row> tornadoBruteResults = null;
		LOGGER.warn("------------ Running Brute force spatial join. ----------");
		mark = System.currentTimeMillis();
		try {
			SimpleFeatureDataFrame hailFrame = new SimpleFeatureDataFrame(session);
			SimpleFeatureDataFrame tornadoFrame = new SimpleFeatureDataFrame(session);
			DataAdapter<?> hailAdapter = dataStore.createAdapterStore().getAdapter(hail_adapter);
			DataAdapter<?> tornadoAdapter = dataStore.createAdapterStore().getAdapter(tornado_adapter);

			JavaPairRDD<GeoWaveInputKey, SimpleFeature> hailRDD = GeoWaveRDD.rddForSimpleFeatures(
					sc.sc(), 
					dataStore, 
					null, 
					new QueryOptions(hailAdapter));
			hailFrame.init(dataStore, hail_adapter);
			hailFrame.getDataFrame(hailRDD).createOrReplaceTempView("hail");
			
			
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> tornadoRDD = GeoWaveRDD.rddForSimpleFeatures(
					sc.sc(), 
					dataStore, 
					null, 
					new QueryOptions(tornadoAdapter));
			tornadoFrame.init(dataStore, tornado_adapter);
			tornadoFrame.getDataFrame(tornadoRDD).createOrReplaceTempView("tornado");
			
			tornadoBruteResults = session.sql("select tornado.* from hail, tornado where geomIntersects(hail.geom,tornado.geom)");
			tornadoBruteResults = tornadoBruteResults.dropDuplicates();
			tornadoBruteCount = tornadoBruteResults.count();
			
			hailBruteResults = session.sql("select hail.* from hail, tornado where geomIntersects(hail.geom,tornado.geom)");
			hailBruteResults = hailBruteResults.dropDuplicates();
			hailBruteCount = hailBruteResults.count();
			
		}
		catch (IOException e) {
			LOGGER.error("Could not load hail dataset into RDD");
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			sc.close();
			Assert.fail();
		}
		dur = (System.currentTimeMillis() - mark);
		
		LOGGER.warn("Indexed Tornado join count= " + tornadoIndexedCount);
		LOGGER.warn("Indexed Hail join count= " + hailIndexedCount);
		LOGGER.warn("Indexed join duration = " + indexJoinDur + " ms.");
		
		LOGGER.warn("Brute tornado join count= " + tornadoBruteCount);
		LOGGER.warn("Brute hail join count= " + hailBruteCount);
		LOGGER.warn("Brute join duration = " + dur + " ms.");
		

	
		TestUtils.deleteAll(dataStore);
		sc.close();
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}