package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CommandLineOptions.OptionMapWrapper;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Assert;
import org.junit.Test;

import com.vividsolutions.jts.geom.Geometry;

public class GeoWaveNNIT extends
		MapReduceTestEnvironment
{

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
		final Map<String, String> options = getAccumuloConfigOptions();
		options.put(
				GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY,
				TEST_NAMESPACE + "_nn");
		final Options nsOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(nsOptions);
		final CommandLineResult<DataStoreCommandLineOptions> dataStoreOptions = DataStoreCommandLineOptions.parseOptions(
				nsOptions,
				new OptionMapWrapper(
						options));
		final CommandLineResult<IndexStoreCommandLineOptions> indexStoreOptions = IndexStoreCommandLineOptions.parseOptions(
				nsOptions,
				new OptionMapWrapper(
						options));
		final CommandLineResult<AdapterStoreCommandLineOptions> adapterStoreOptions = AdapterStoreCommandLineOptions.parseOptions(
				nsOptions,
				new OptionMapWrapper(
						options));
		dataGenerator.setIncludePolygons(false);
		ingest(dataStoreOptions.getResult().createStore());
		runNN(
				new SpatialQuery(
						dataGenerator.getBoundingRegion()),
				dataStoreOptions.getResult(),
				indexStoreOptions.getResult(),
				adapterStoreOptions.getResult());
	}

	private void runNN(
			final DistributableQuery query,
			final DataStoreCommandLineOptions dataStoreOptions,
			final IndexStoreCommandLineOptions indexStoreOptions,
			final AdapterStoreCommandLineOptions adapterStoreOptions )
			throws Exception {

		final NNJobRunner jobRunner = new NNJobRunner();
		final int res = jobRunner.run(
				getConfiguration(),
				new PropertyManagement(
						new ParameterEnum[] {
							ExtractParameters.Extract.QUERY,
							ExtractParameters.Extract.MIN_INPUT_SPLIT,
							ExtractParameters.Extract.MAX_INPUT_SPLIT,
							PartitionParameters.Partition.PARTITION_DISTANCE,
							ClusteringParameters.Clustering.DISTANCE_THRESHOLDS,
							PartitionParameters.Partition.PARTITIONER_CLASS,
							StoreParam.DATA_STORE,
							StoreParam.INDEX_STORE,
							StoreParam.ADAPTER_STORE,
							OutputParameters.Output.HDFS_OUTPUT_PATH,
							MapReduceParameters.MRConfig.HDFS_BASE_DIR,
							OutputParameters.Output.REDUCER_COUNT,
							OutputParameters.Output.OUTPUT_FORMAT,
							InputParameters.Input.INPUT_FORMAT
						},
						new Object[] {
							query,
							Integer.toString(MIN_INPUT_SPLITS),
							Integer.toString(MAX_INPUT_SPLITS),
							0.2,
							"0.2,0.2",
							OrthodromicDistancePartitioner.class,
							new PersistableDataStore(
									dataStoreOptions),
							new PersistableIndexStore(
									indexStoreOptions),
							new PersistableAdapterStore(
									adapterStoreOptions),
							hdfsBaseDirectory + "/t1/pairs",
							hdfsBaseDirectory + "/t1",
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
		final FileSystem fs = FileSystem.get(getConfiguration());
		final FileStatus[] fss = fs.listStatus(new Path(
				hdfsBaseDirectory + "/t1/pairs"));
		for (final FileStatus ifs : fss) {
			if (ifs.isFile() && ifs.getPath().toString().matches(
					".*part-r-0000[0-9]")) {
				try (SequenceFile.Reader reader = new SequenceFile.Reader(
						getConfiguration(),
						Reader.file(ifs.getPath()))) {

					final Text key = new Text();
					final Text val = new Text();

					while (reader.next(
							key,
							val)) {
						count++;
						System.err.println(key + "\t" + val);
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
