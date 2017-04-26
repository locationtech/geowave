package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ShapefileTool;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.DBScanIterationsJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
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
public class DBScanIT
{
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(DBScanIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING DBScanIT              *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED DBScanIT                *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setSRS(ClusteringUtils.CLUSTERING_CRS);
		try {
			typeBuilder.setCRS(CRS.decode(
					ClusteringUtils.CLUSTERING_CRS,
					true));
		}
		catch (final FactoryException e) {
			e.printStackTrace();
			return null;
		}
		// add attributes in order
		typeBuilder.add(
				"geom",
				Point.class);
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
			new FeatureCentroidOrthodromicDistanceFn(),
			getBuilder());

	@Test
	public void testDBScan()
			throws Exception {
		TestUtils.deleteAll(dataStorePluginOptions);
		dataGenerator.setIncludePolygons(false);
		ingest(dataStorePluginOptions.createDataStore());
		runScan(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
	}

	private void runScan(
			final DistributableQuery query )
			throws Exception {

		final DBScanIterationsJobRunner jobRunner = new DBScanIterationsJobRunner();
		final Configuration conf = MapReduceTestUtils.getConfiguration();
		final int res = jobRunner.run(
				conf,
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.QUERY_OPTIONS,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							PartitionParameters.Partition.MAX_DISTANCE,
							PartitionParameters.Partition.PARTITIONER_CLASS,
							ClusteringParameters.Clustering.MINIMUM_SIZE,
							StoreParam.INPUT_STORE,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							OutputParameters.Output.REDUCER_COUNT,
							InputParameters.Input.INPUT_FORMAT,
							GlobalParameters.Global.BATCH_ID,
							PartitionParameters.Partition.PARTITION_DECREASE_RATE,
							PartitionParameters.Partition.PARTITION_PRECISION
						},
						new Object[] {
							query,
							new QueryOptions(),
							Integer.toString(MapReduceTestUtils.MIN_INPUT_SPLITS),
							Integer.toString(MapReduceTestUtils.MAX_INPUT_SPLITS),
							10000.0,
							OrthodromicDistancePartitioner.class,
							10,
							new PersistableStore(
									dataStorePluginOptions),
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t1",
							2,
							GeoWaveInputFormatConfiguration.class,
							"bx5",
							0.15,
							0.95
						}));

		Assert.assertEquals(
				0,
				res);

		Assert.assertTrue(readHulls() > 2);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int readHulls()
			throws Exception {
		final CentroidManager<SimpleFeature> centroidManager = new CentroidManagerGeoWave<SimpleFeature>(
				dataStorePluginOptions.createDataStore(),
				dataStorePluginOptions.createIndexStore(),
				dataStorePluginOptions.createAdapterStore(),
				new SimpleFeatureItemWrapperFactory(),
				"concave_hull",
				new SpatialDimensionalityTypeProvider().createPrimaryIndex().getId().getString(),
				"bx5",
				0);

		int count = 0;
		for (final String grp : centroidManager.getAllCentroidGroups()) {
			for (final AnalyticItemWrapper<SimpleFeature> feature : centroidManager.getCentroidsForGroup(grp)) {
				ShapefileTool.writeShape(
						feature.getName(),
						new File(
								"./target/test_final_" + feature.getName()),
						new Geometry[] {
							feature.getGeometry()
						});
				count++;

			}
		}
		return count;
	}

	private void ingest(
			final DataStore dataStore )
			throws IOException {
		final List<SimpleFeature> features = dataGenerator.generatePointSet(
				0.05,
				0.5,
				4,
				800,
				new double[] {
					-86,
					-30
				},
				new double[] {
					-90,
					-34
				});

		features.addAll(dataGenerator.generatePointSet(
				dataGenerator.getFactory().createLineString(
						new Coordinate[] {
							new Coordinate(
									-87,
									-32),
							new Coordinate(
									-87.5,
									-32.3),
							new Coordinate(
									-87.2,
									-32.7)
						}),
				0.2,
				500));

		ShapefileTool.writeShape(
				new File(
						"./target/test_in"),
				features);
		dataGenerator.writeToGeoWave(
				dataStore,
				features);
	}
}