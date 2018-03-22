package mil.nga.giat.geowave.test.spark;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

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

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
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
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomWithinDistance;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.wkt.GeomFunctionRegistry;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytic.spark.spatial.TieredSpatialJoin;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveSparkSpatialJoinIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSparkSpatialJoinIT.class);

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.ACCUMULO
	})
	protected DataStorePluginOptions dataStore;

	private static long startMillis;
	private static SparkSession session;
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> hailRDD = null;
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> tornadoRDD = null;
	private Dataset<Row> hailBruteResults = null;
	private long hailBruteCount = 0;
	private Dataset<Row> tornadoBruteResults = null;
	private long tornadoBruteCount = 0;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		session = SparkSession.builder().appName(
				"SpatialJoinTest").master(
				"local[*]").config(
				"spark.serializer",
				"org.apache.spark.serializer.KryoSerializer").config(
				"spark.kryo.registrator",
				"mil.nga.giat.geowave.analytic.spark.GeoWaveRegistrator").getOrCreate();

		GeomFunctionRegistry.registerGeometryFunctions(session);
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveSparkSpatialJoinIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");

	}

	@AfterClass
	public static void reportTestFinish() {
		
		session.close();
		
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
	public void testHailTornadoDistanceJoin() {
		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());
		long mark = System.currentTimeMillis();
		ingestHailandTornado();
		long dur = (System.currentTimeMillis() - mark);

		NumericIndexStrategy strategy = createIndexStrategy();

		TieredSpatialJoin tieredJoin = new TieredSpatialJoin();
		ByteArrayId hail_adapter = new ByteArrayId(
				"hail");
		ByteArrayId tornado_adapter = new ByteArrayId(
				"tornado_tracks");
		GeomWithinDistance distancePredicate = new GeomWithinDistance(
				0.01);
		String sqlHail = "select hail.* from hail, tornado where geomDistance(hail.geom,tornado.geom) <= 0.01";
		String sqlTornado = "select tornado.* from hail, tornado where geomDistance(hail.geom,tornado.geom) <= 0.01";

		loadRDDs(
				hail_adapter,
				tornado_adapter);
		

		this.hailRDD.cache();
		this.tornadoRDD.cache();

		long tornadoIndexedCount = 0;
		long hailIndexedCount = 0;
		LOGGER.warn("------------ Running indexed spatial join. ----------");
		mark = System.currentTimeMillis();
		try {
			tieredJoin.join(
					session,
					hailRDD,
					tornadoRDD,
					distancePredicate,
					strategy);
		}
		catch (InterruptedException e) {
			LOGGER.error("Async error in join");
			e.printStackTrace();
		}
		catch (ExecutionException e) {
			LOGGER.error("Async error in join");
			e.printStackTrace();
		}
		hailIndexedCount = tieredJoin.getLeftResults().count();
		tornadoIndexedCount = tieredJoin.getRightResults().count();
		long indexJoinDur = (System.currentTimeMillis() - mark);
		LOGGER.warn("Indexed Result Count: " + (hailIndexedCount + tornadoIndexedCount));
		SimpleFeatureDataFrame indexHailFrame = new SimpleFeatureDataFrame(
				session);
		SimpleFeatureDataFrame indexTornadoFrame = new SimpleFeatureDataFrame(
				session);

		indexTornadoFrame.init(
				dataStore,
				tornado_adapter);
		Dataset<Row> indexedTornado = indexTornadoFrame.getDataFrame(tieredJoin.getRightResults());

		indexHailFrame.init(
				dataStore,
				hail_adapter);
		Dataset<Row> indexedHail = indexHailFrame.getDataFrame(tieredJoin.getLeftResults());

		LOGGER.warn("------------ Running Brute force spatial join. ----------");
		dur = runBruteForceJoin(
				hail_adapter,
				tornado_adapter,
				sqlHail,
				sqlTornado);

		LOGGER.warn("Indexed join duration = " + indexJoinDur + " ms.");
		LOGGER.warn("Brute join duration = " + dur + " ms.");

		// Verify each row matches
		Assert.assertTrue((hailIndexedCount == hailBruteCount));
		Assert.assertTrue((tornadoIndexedCount == tornadoBruteCount));
		Assert.assertTrue(
				"Subtraction between brute force join and indexed Hail should result in count of 0",
				(indexedHail.except(
						hailBruteResults).count() == 0));
		Assert.assertTrue(
				"Subtraction between brute force join and indexed Tornado should result in count of 0",
				(indexedTornado.except(
						tornadoBruteResults).count() == 0));

		hailRDD.unpersist();
		tornadoRDD.unpersist();
		TestUtils.deleteAll(dataStore);
	}

	private NumericIndexStrategy createIndexStrategy() {
		SpatialIndexBuilder indexProvider = new SpatialIndexBuilder();
		PrimaryIndex index = indexProvider.createIndex();
		return index.getIndexStrategy();
	}

	private void ingestHailandTornado() {
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

	}

	private void loadRDDs(ByteArrayId hail_adapter, ByteArrayId tornado_adapter) {
		DataAdapter<?> hailAdapter = dataStore.createAdapterStore().getAdapter(hail_adapter);
		DataAdapter<?> tornadoAdapter = dataStore.createAdapterStore().getAdapter(tornado_adapter);
		try {
			hailRDD = GeoWaveRDD.rddForSimpleFeatures(
					session.sparkContext(), 
					dataStore, 
					null, 
					new QueryOptions(hailAdapter)).reduceByKey((f1,f2) -> f1);
			hailRDD.cache();

			tornadoRDD = GeoWaveRDD.rddForSimpleFeatures(
					session.sparkContext(), 
					dataStore, 
					null, 
					new QueryOptions(tornadoAdapter)).reduceByKey((f1,f2) -> f1);
			tornadoRDD.cache();
		}
		catch (IOException e) {
			LOGGER.error("Could not load rdds for test");
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail();
		}
	}

	private long runBruteForceJoin(
			ByteArrayId hail_adapter,
			ByteArrayId tornado_adapter,
			String sqlHail,
			String sqlTornado ) {
		long mark = System.currentTimeMillis();
		SimpleFeatureDataFrame hailFrame = new SimpleFeatureDataFrame(
				session);
		SimpleFeatureDataFrame tornadoFrame = new SimpleFeatureDataFrame(
				session);

		tornadoFrame.init(
				dataStore,
				tornado_adapter);
		tornadoFrame.getDataFrame(
				tornadoRDD).createOrReplaceTempView(
				"tornado");

		hailFrame.init(
				dataStore,
				hail_adapter);
		hailFrame.getDataFrame(
				hailRDD).createOrReplaceTempView(
				"hail");

		hailBruteResults = session.sql(sqlHail);
		hailBruteResults = hailBruteResults.dropDuplicates();
		hailBruteResults.cache();
		hailBruteCount = hailBruteResults.count();

		tornadoBruteResults = session.sql(sqlTornado);
		tornadoBruteResults = tornadoBruteResults.dropDuplicates();
		tornadoBruteResults.cache();
		tornadoBruteCount = tornadoBruteResults.count();
		long dur = (System.currentTimeMillis() - mark);
		LOGGER.warn("Brute Result Count: " + (tornadoBruteCount + hailBruteCount));
		return dur;
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}