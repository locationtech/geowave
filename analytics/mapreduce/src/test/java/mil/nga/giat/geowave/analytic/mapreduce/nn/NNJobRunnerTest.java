package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.io.IOException;
import java.util.HashMap;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.distance.GeometryCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.param.ParameterHelper;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryIndexStoreFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NNJobRunnerTest
{
	final NNJobRunner jjJobRunner = new NNJobRunner();
	final PropertyManagement runTimeProperties = new PropertyManagement();
	private static final String TEST_NAMESPACE = "test";

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
				return ToolRunner.run(
						configuration,
						tool,
						new String[] {});
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
				final ScopedJobConfiguration configWrapper = new ScopedJobConfiguration(
						job.getConfiguration(),
						NNMapReduce.class);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));

				Assert.assertEquals(
						0.4,
						configWrapper.getDouble(
								Partition.PARTITION_DISTANCE,
								0.0),
						0.001);

				Assert.assertEquals(
						100,
						configWrapper.getInt(
								Partition.MAX_MEMBER_SELECTION,

								1));

				try {
					final Partitioner<?> wrapper = configWrapper.getInstance(
							Partition.PARTITIONER_CLASS,
							Partitioner.class,
							null);

					Assert.assertEquals(
							OrthodromicDistancePartitioner.class,
							wrapper.getClass());

					final Partitioner<?> secondary = configWrapper.getInstance(
							Partition.SECONDARY_PARTITIONER_CLASS,
							Partitioner.class,
							null);

					Assert.assertEquals(
							OrthodromicDistancePartitioner.class,
							secondary.getClass());

					final DistanceFn<?> distancFn = configWrapper.getInstance(
							CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
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

			@Override
			public Configuration getConfiguration(
					final PropertyManagement runTimeProperties )
					throws IOException {
				return new Configuration();
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
				StoreParam.DATA_STORE,
				new PersistableDataStore(
						new DataStoreCommandLineOptions(
								new MemoryDataStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));

		runTimeProperties.store(
				StoreParam.ADAPTER_STORE,
				new PersistableAdapterStore(
						new AdapterStoreCommandLineOptions(
								new MemoryAdapterStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));

		runTimeProperties.store(
				StoreParam.INDEX_STORE,
				new PersistableIndexStore(
						new IndexStoreCommandLineOptions(
								new MemoryIndexStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));

		runTimeProperties.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		runTimeProperties.store(
				Partition.PARTITIONER_CLASS,
				OrthodromicDistancePartitioner.class);

		runTimeProperties.store(
				Partition.SECONDARY_PARTITIONER_CLASS,
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
}
