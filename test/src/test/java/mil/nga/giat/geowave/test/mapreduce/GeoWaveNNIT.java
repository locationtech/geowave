package mil.nga.giat.geowave.test.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
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

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNJobRunner;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.DataStore;
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
public class GeoWaveNNIT
{

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveNNIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING GeoWaveNNIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED GeoWaveNNIT             *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	private SimpleFeatureBuilder getBuilder() {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("testnn");
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
			new FeatureCentroidOrthodromicDistanceFn(),
			getBuilder());

	@Test
	public void testNN()
			throws Exception {
		// Clear out temp directories that need to be empty
		MapReduceTestEnvironment.getInstance().tearDown();
		TestUtils.deleteAll(dataStorePluginOptions);
		dataGenerator.setIncludePolygons(false);
		ingest(dataStorePluginOptions.createDataStore());
		runNN(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
		TestUtils.deleteAll(dataStorePluginOptions);
	}

	private void runNN(
			final DistributableQuery query )
			throws Exception {

		final NNJobRunner jobRunner = new NNJobRunner();

		// final int res = 1;
		// GeoWaveMain.main(new String[] {
		// "analytic",
		// "nn",
		// "--query.adapters",
		// "testnn",
		// "--query.index",
		// new
		// SpatialDimensionalityTypeProvider().createPrimaryIndex().getId().getString(),
		// "-emn",
		// Integer.toString(MIN_INPUT_SPLITS),
		// "-emx",
		// Integer.toString(MAX_INPUT_SPLITS),
		// "-pmd",
		// "0.2",
		// "-pdt",
		// "0.2,0.2",
		// "-pc",
		// OrthodromicDistancePartitioner.class.toString(),
		// "-oop",
		// hdfsBaseDirectory + "/t1/pairs",
		// "-hdfsbase",
		// hdfsBaseDirectory + "/t1",
		// "-orc",
		// "3",
		// "-ofc",
		// SequenceFileOutputFormatConfiguration.class.toString(),
		// "-ifc",
		// GeoWaveInputFormatConfiguration.class.toString(),
		// "foo"
		// });
		final int res = jobRunner.run(
				MapReduceTestUtils.getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							PartitionParameters.Partition.MAX_DISTANCE,
							PartitionParameters.Partition.DISTANCE_THRESHOLDS,
							PartitionParameters.Partition.PARTITIONER_CLASS,
							StoreParam.INPUT_STORE,
							OutputParameters.Output.HDFS_OUTPUT_PATH,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							OutputParameters.Output.REDUCER_COUNT,
							OutputParameters.Output.OUTPUT_FORMAT,
							InputParameters.Input.INPUT_FORMAT
						},
						new Object[] {
							query,
							Integer.toString(MapReduceTestUtils.MIN_INPUT_SPLITS),
							Integer.toString(MapReduceTestUtils.MAX_INPUT_SPLITS),
							0.2,
							"0.2,0.2",
							OrthodromicDistancePartitioner.class,
							new PersistableStore(
									dataStorePluginOptions),
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY
									+ "/t1/pairs",
							TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t1",
							3,
							SequenceFileOutputFormatConfiguration.class,
							GeoWaveInputFormatConfiguration.class
						}));

		Assert.assertEquals(
				0,
				res);

		Assert.assertTrue(readFile() > 0);
		// for travis-ci to run, we want to limit the memory consumption
		System.gc();
	}

	private int readFile()
			throws IllegalArgumentException,
			IOException {
		int count = 0;
		final FileSystem fs = FileSystem.get(MapReduceTestUtils.getConfiguration());
		final FileStatus[] fss = fs.listStatus(new Path(
				TestUtils.TEMP_DIR + File.separator + MapReduceTestEnvironment.HDFS_BASE_DIRECTORY + "/t1/pairs"));
		for (final FileStatus ifs : fss) {
			if (ifs.isFile() && ifs.getPath().toString().matches(
					".*part-r-0000[0-9]")) {
				try (SequenceFile.Reader reader = new SequenceFile.Reader(
						MapReduceTestUtils.getConfiguration(),
						Reader.file(ifs.getPath()))) {

					final Text key = new Text();
					final Text val = new Text();

					while (reader.next(
							key,
							val)) {
						count++;
					}
				}
			}
		}
		return count;
	}

	private void ingest(
			final DataStore dataStore )
			throws IOException {

		dataGenerator.writeToGeoWave(
				dataStore,
				dataGenerator.generatePointSet(
						0.00002,
						0.02,
						3,
						800,
						new double[] {
							-92,
							-37
						},
						new double[] {
							-90,
							-35
						}));
	}
}
