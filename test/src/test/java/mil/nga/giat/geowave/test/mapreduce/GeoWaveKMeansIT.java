package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelJumpKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.JumpParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.MAP_REDUCE
})
public class GeoWaveKMeansIT
{
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveKMeansIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoWaveKMeansIT       *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveKMeansIT         *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate
														// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Geometry.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		return new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());
	}

	final GeometryDataSetGenerator dataGenerator = new GeometryDataSetGenerator(
			new FeatureCentroidDistanceFn(),
			getBuilder());

	private void testIngest(
			final DataStore dataStore )
			throws IOException {

		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						3,
						800,
						new double[] {
							-100,
							-45
						},
						new double[] {
							-90,
							-35
						}));
		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						6,
						600,
						new double[] {
							0,
							0
						},
						new double[] {
							10,
							10
						}));
		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.15,
						0.2,
						4,
						900,
						new double[] {
							65,
							35
						},
						new double[] {
							75,
							45
						}));

	}

	@Test
	public void testIngestAndQueryGeneralGpx()
			throws Exception {
		TestUtils.deleteAll(dataStorePluginOptions);
		testIngest(dataStorePluginOptions.createDataStore());

		runKPlusPlus(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
	}

	private void runKPlusPlus(
			final DistributableQuery query )
			throws Exception {

		final MultiLevelKMeansClusteringJobRunner jobRunner = new MultiLevelKMeansClusteringJobRunner();
		final int res = jobRunner.run(
				MapReduceTestUtils.getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							ClusteringParameters.Clustering.ZOOM_LEVELS,
							ClusteringParameters.Clustering.MAX_ITERATIONS,
							ClusteringParameters.Clustering.RETAIN_GROUP_ASSIGNMENTS,
							ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
							StoreParam.INPUT_STORE,
							GlobalParameters.Global.BATCH_ID,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							SampleParameters.Sample.MAX_SAMPLE_SIZE,
							SampleParameters.Sample.MIN_SAMPLE_SIZE
						},
						new Object[] {
							query,
							MapReduceTestUtils.MIN_INPUT_SPLITS,
							MapReduceTestUtils.MAX_INPUT_SPLITS,
							2,
							2,
							false,
							"centroid",
							new PersistableStore(
									dataStorePluginOptions),
							"bx1",
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t1",
							3,
							2
						}));

		Assert.assertEquals(
				0,
				res);

		final DataStore dataStore = dataStorePluginOptions.createDataStore();
		final IndexStore indexStore = dataStorePluginOptions.createIndexStore();
		final AdapterStore adapterStore = dataStorePluginOptions.createAdapterStore();
		final int resultCounLevel1 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				"bx1",
				1, // level
				1);
		final int resultCounLevel2 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				"bx1",
				2, // level
				resultCounLevel1);
		Assert.assertTrue(resultCounLevel2 >= 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private void runKJumpPlusPlus(
			final DistributableQuery query )
			throws Exception {

		final MultiLevelJumpKMeansClusteringJobRunner jobRunner2 = new MultiLevelJumpKMeansClusteringJobRunner();
		final int res2 = jobRunner2.run(
				MapReduceTestUtils.getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							ClusteringParameters.Clustering.ZOOM_LEVELS,
							ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
							StoreParam.INPUT_STORE,
							GlobalParameters.Global.BATCH_ID,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							JumpParameters.Jump.RANGE_OF_CENTROIDS,
							JumpParameters.Jump.KPLUSPLUS_MIN,
							ClusteringParameters.Clustering.MAX_ITERATIONS
						},
						new Object[] {
							query,
							MapReduceTestUtils.MIN_INPUT_SPLITS,
							MapReduceTestUtils.MAX_INPUT_SPLITS,
							2,
							"centroid",
							new PersistableStore(
									dataStorePluginOptions),
							"bx2",
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t2",
							new NumericRange(
									4,
									7),
							5,
							2
						}));

		Assert.assertEquals(
				0,
				res2);

		final DataStore dataStore = dataStorePluginOptions.createDataStore();
		final IndexStore indexStore = dataStorePluginOptions.createIndexStore();
		final AdapterStore adapterStore = dataStorePluginOptions.createAdapterStore();
		final int jumpRresultCounLevel1 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				"bx2",
				1,
				1);
		final int jumpRresultCounLevel2 = countResults(
				dataStore,
				indexStore,
				adapterStore,
				"bx2",
				2,
				jumpRresultCounLevel1);
		Assert.assertTrue(jumpRresultCounLevel1 >= 2);
		Assert.assertTrue(jumpRresultCounLevel2 >= 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int countResults(
			final DataStore dataStore,
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final String batchID,
			final int level,
			final int expectedParentCount )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {

		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				"centroid",
				TestUtils.DEFAULT_SPATIAL_INDEX.getId().getString(),
				batchID,
				level);

		final CentroidManager<SimpleFeature> hullManager = new CentroidManagerGeoWave<SimpleFeature>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				"convex_hull",
				TestUtils.DEFAULT_SPATIAL_INDEX.getId().getString(),
				batchID,
				level);

		int childCount = 0;
		int parentCount = 0;
		for (final String grp : centroidManager.getAllCentroidGroups()) {
			final List<AnalyticItemWrapper<SimpleFeature>> centroids = centroidManager.getCentroidsForGroup(grp);
			final List<AnalyticItemWrapper<SimpleFeature>> hulls = hullManager.getCentroidsForGroup(grp);

			for (final AnalyticItemWrapper<SimpleFeature> centroid : centroids) {
				if (centroid.getAssociationCount() == 0) {
					continue;
				}
				Assert.assertTrue(centroid.getGeometry() != null);
				Assert.assertTrue(centroid.getBatchID() != null);
				boolean found = false;
				final List<SimpleFeature> features = new ArrayList<SimpleFeature>();
				for (final AnalyticItemWrapper<SimpleFeature> hull : hulls) {
					found |= (hull.getName().equals(centroid.getName()));
					Assert.assertTrue(hull.getGeometry() != null);
					Assert.assertTrue(hull.getBatchID() != null);
					features.add(hull.getWrappedItem());
				}
				System.out.println(features);
				Assert.assertTrue(
						grp,
						found);
				childCount++;
			}
			parentCount++;
		}
		Assert.assertEquals(
				batchID,
				expectedParentCount,
				parentCount);
		return childCount;

	}
}
