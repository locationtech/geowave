package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

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
			new FeatureCentroidOrthodromicDistanceFn(),
			getBuilder());

	@Test
	public void testNN()
			throws Exception {
		dataGenerator.setIncludePolygons(false);
		ingest();
		runNN(new SpatialQuery(
				dataGenerator.getBoundingRegion()));
	}

	private void runNN(
			final DistributableQuery query )
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
							GlobalParameters.Global.ZOOKEEKER,
							GlobalParameters.Global.ACCUMULO_INSTANCE,
							GlobalParameters.Global.ACCUMULO_USER,
							GlobalParameters.Global.ACCUMULO_PASSWORD,
							GlobalParameters.Global.ACCUMULO_NAMESPACE,
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
							zookeeper,
							accumuloInstance,
							accumuloUser,
							accumuloPassword,
							TEST_NAMESPACE,
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

	private void ingest()
			throws IOException {

		dataGenerator.writeToGeoWave(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				TEST_NAMESPACE,
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
