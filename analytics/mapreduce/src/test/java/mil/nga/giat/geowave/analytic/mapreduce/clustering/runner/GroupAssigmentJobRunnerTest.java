package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.distance.GeometryCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.GroupAssignmentMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.partitioner.FeatureDataAdapterStoreFactory;

import org.apache.commons.cli.Option;
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

public class GroupAssigmentJobRunnerTest
{

	final GroupAssigmentJobRunner runner = new GroupAssigmentJobRunner();
	final PropertyManagement runTimeProperties = new PropertyManagement();

	@Before
	public void init() {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroidtest",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getType();

		runner.setMapReduceIntegrater(new MapReduceIntegration() {
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
						3,
						configWrapper.getInt(
								CentroidParameters.Centroid.ZOOM_LEVEL,
								NestedGroupCentroidAssignment.class,
								-1));
				Assert.assertEquals(
						"b1234",
						configWrapper.getString(
								GlobalParameters.Global.PARENT_BATCH_ID,
								NestedGroupCentroidAssignment.class,
								""));
				Assert.assertEquals(
						"b12345",
						configWrapper.getString(
								GlobalParameters.Global.BATCH_ID,
								NestedGroupCentroidAssignment.class,
								""));

				try {
					final AnalyticItemWrapperFactory<?> wrapper = configWrapper.getInstance(
							CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
							GroupAssignmentMapReduce.class,
							AnalyticItemWrapperFactory.class,
							SimpleFeatureItemWrapperFactory.class);

					Assert.assertEquals(
							SimpleFeatureItemWrapperFactory.class,
							wrapper.getClass());

					final DistanceFn<?> distancFn = configWrapper.getInstance(
							CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
							NestedGroupCentroidAssignment.class,
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
		runner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				new Path(
						"file://foo/bin")));
		runner.setZoomLevel(3);
		runner.setReducerCount(10);

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
				GlobalParameters.Global.BATCH_ID,
				"b12345");
		runTimeProperties.store(
				GlobalParameters.Global.PARENT_BATCH_ID,
				"b1234");

		runTimeProperties.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		FeatureDataAdapterStoreFactory.saveState(
				new FeatureDataAdapter(
						ftype),
				runTimeProperties);
	}

	@Test
	public void test()
			throws Exception {

		runner.run(runTimeProperties);
	}

	@Test
	public void testOptions() {
		final Set<Option> options = new HashSet<Option>();
		runner.fillOptions(options);

		assertTrue(PropertyManagement.hasOption(
				options,
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS));

		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.PARENT_BATCH_ID));

		assertTrue(PropertyManagement.hasOption(
				options,
				CentroidParameters.Centroid.ZOOM_LEVEL));

		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.ACCUMULO_INSTANCE));

		assertTrue(PropertyManagement.hasOption(
				options,
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS));
	}

}
