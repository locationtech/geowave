package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.distance.GeometryCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.GroupAssignmentMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.param.ParameterHelper;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

public class GroupAssigmentJobRunnerTest
{

	final GroupAssigmentJobRunner runner = new GroupAssigmentJobRunner();
	final PropertyManagement runTimeProperties = new PropertyManagement();
	private static final String TEST_NAMESPACE = "test";

	@Before
	public void init() {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroidtest",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();

		runner.setMapReduceIntegrater(new MapReduceIntegration() {
			@Override
			public int submit(
					final Configuration configuration,
					final PropertyManagement runTimeProperties,
					final GeoWaveAnalyticJobRunner tool )
					throws Exception {
				tool.setConf(configuration);
				((ParameterHelper<Object>) StoreParam.INPUT_STORE.getHelper()).setValue(
						configuration,
						GroupAssignmentMapReduce.class,
						StoreParam.INPUT_STORE.getHelper().getValue(
								runTimeProperties));
				return tool.run(new String[] {});
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
						GroupAssignmentMapReduce.class);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));

				Assert.assertEquals(
						3,
						configWrapper.getInt(
								CentroidParameters.Centroid.ZOOM_LEVEL,
								-1));
				Assert.assertEquals(
						"b1234",
						configWrapper.getString(
								GlobalParameters.Global.PARENT_BATCH_ID,
								""));
				Assert.assertEquals(
						"b12345",
						configWrapper.getString(
								GlobalParameters.Global.BATCH_ID,
								""));

				try {
					final AnalyticItemWrapperFactory<?> wrapper = configWrapper.getInstance(
							CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
							AnalyticItemWrapperFactory.class,
							SimpleFeatureItemWrapperFactory.class);

					Assert.assertEquals(
							SimpleFeatureItemWrapperFactory.class,
							wrapper.getClass());

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
		runner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				new Path(
						"file://foo/bin")));
		runner.setZoomLevel(3);
		runner.setReducerCount(10);

		runTimeProperties.store(
				MRConfig.HDFS_BASE_DIR,
				"/");

		runTimeProperties.store(
				GlobalParameters.Global.BATCH_ID,
				"b12345");
		runTimeProperties.store(
				GlobalParameters.Global.PARENT_BATCH_ID,
				"b1234");

		runTimeProperties.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		pluginOptions.selectPlugin("memory");
		MemoryRequiredOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
		opts.setGeowaveNamespace(TEST_NAMESPACE);
		PersistableStore store = new PersistableStore(
				pluginOptions);

		runTimeProperties.store(
				StoreParam.INPUT_STORE,
				store);

		pluginOptions.createAdapterStore().addAdapter(
				new FeatureDataAdapter(
						ftype));
	}

	@Test
	public void test()
			throws Exception {

		runner.run(runTimeProperties);
	}
}
