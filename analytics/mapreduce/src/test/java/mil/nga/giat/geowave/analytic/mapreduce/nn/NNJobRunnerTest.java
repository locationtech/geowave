package mil.nga.giat.geowave.analytic.mapreduce.nn;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.distance.GeometryCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytic.partitioner.FeatureDataAdapterStoreFactory;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NNJobRunnerTest
{
	final NNJobRunner jjJobRunner = new NNJobRunner();
	final PropertyManagement runTimeProperties = new PropertyManagement();

	@Before
	public void init() {
		jjJobRunner.setMapReduceIntegrater(new MapReduceIntegration() {
			@Override
			public int submit(
					final Configuration configuration,
					final PropertyManagement runTimeProperties,
					final GeoWaveAnalyticJobRunner tool )
					throws Exception {
				tool.setConf(configuration);
				FeatureDataAdapterStoreFactory.transferState(
						configuration,
						runTimeProperties);
				return tool.run(runTimeProperties.toGeoWaveRunnerArguments());
			}

			@Override
			public Counters waitForCompletion(
					final Job job )
					throws ClassNotFoundException,
					IOException,
					InterruptedException {

				Assert.assertEquals(
						SequenceFileInputFormat.class,
						job.getInputFormatClass());
				Assert.assertEquals(
						10,
						job.getNumReduceTasks());
				final JobContextConfigurationWrapper configWrapper = new JobContextConfigurationWrapper(
						job);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));

				Assert.assertEquals(
						0.4,
						configWrapper.getDouble(
								Partition.PARTITION_DISTANCE,
								NNMapReduce.class,
								0.0),
						0.001);

				Assert.assertEquals(
						100,
						configWrapper.getInt(
								Partition.MAX_MEMBER_SELECTION,
								NNMapReduce.class,
								1));

				try {
					final Partitioner<?> wrapper = configWrapper.getInstance(
							Partition.PARTITIONER_CLASS,
							NNMapReduce.class,
							Partitioner.class,
							null);

					Assert.assertEquals(
							OrthodromicDistancePartitioner.class,
							wrapper.getClass());

					final DistanceFn<?> distancFn = configWrapper.getInstance(
							CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
							NNMapReduce.class,
							DistanceFn.class,
							GeometryCentroidDistanceFn.class);

					Assert.assertEquals(
							FeatureCentroidDistanceFn.class,
							distancFn.getClass());

				}
				catch (final InstantiationException e) {
					throw new IOException(
							"Unable to configure system",
							e);
				}
				catch (final IllegalAccessException e) {
					throw new IOException(
							"Unable to configure system",
							e);
				}

				Assert.assertEquals(
						10,
						job.getNumReduceTasks());

				return new Counters();
			}

			@Override
			public Job getJob(
					final Tool tool )
					throws IOException {
				return new Job(
						tool.getConf());
			}
		});

		jjJobRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				new Path(
						"file://foo/bin")));
		jjJobRunner.setReducerCount(10);

		runTimeProperties.store(
				MRConfig.HDFS_BASE_DIR,
				"/");

		runTimeProperties.store(
				GlobalParameters.Global.ZOOKEEKER,
				"localhost:3000");

		runTimeProperties.store(
				GlobalParameters.Global.ACCUMULO_INSTANCE,
				"accumulo");
		runTimeProperties.store(
				GlobalParameters.Global.ACCUMULO_USER,
				"root");
		runTimeProperties.store(
				GlobalParameters.Global.ACCUMULO_PASSWORD,
				"pwd");
		runTimeProperties.store(
				GlobalParameters.Global.ACCUMULO_NAMESPACE,
				"test");

		runTimeProperties.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		runTimeProperties.store(
				Partition.PARTITIONER_CLASS,
				OrthodromicDistancePartitioner.class);

		runTimeProperties.store(
				Partition.PARTITION_DISTANCE,
				Double.valueOf(0.4));

		runTimeProperties.store(
				Partition.MAX_MEMBER_SELECTION,
				Integer.valueOf(100));
	}

	@Test
	public void test()
			throws Exception {

		jjJobRunner.run(runTimeProperties);
	}

	@Test
	public void testOptions() {
		final Set<Option> options = new HashSet<Option>();
		jjJobRunner.fillOptions(options);

		assertTrue(PropertyManagement.hasOption(
				options,
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS));

		assertTrue(PropertyManagement.hasOption(
				options,
				Partition.MAX_MEMBER_SELECTION));

		assertTrue(PropertyManagement.hasOption(
				options,
				Partition.PARTITION_DISTANCE));

		assertTrue(PropertyManagement.hasOption(
				options,
				Partition.PARTITIONER_CLASS));

		/*
		 * 
		 * Should this be part of the test? When options are requested, the
		 * runner does not know the selected partition algorithm.
		 * 
		 * assertTrue(PropertyManagement.hasOption( options,
		 * GlobalParameters.Global.CRS_ID));
		 * 
		 * assertTrue(PropertyManagement.hasOption( options,
		 * ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS));
		 * 
		 * assertTrue(PropertyManagement.hasOption( options,
		 * ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT));
		 * 
		 * assertTrue(PropertyManagement.hasOption( options,
		 * CommonParameters.Common.INDEX_MODEL_BUILDER_CLASS));
		 * 
		 * assertTrue(PropertyManagement.hasOption( options,
		 * ClusteringParameters.Clustering.DISTANCE_THRESHOLDS));
		 */
	}
}
