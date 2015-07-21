package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.db.AccumuloAdapterStoreFactory;
import mil.nga.giat.geowave.analytic.db.AdapterStoreFactory;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.ConvexHullMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.partitioner.FeatureDataAdapterStoreFactory;
import mil.nga.giat.geowave.analytic.partitioner.MemoryIndexStoreFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

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

public class ConvexHullJobRunnerTest
{
	final ConvexHullJobRunner hullRunner = new ConvexHullJobRunner();
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

		hullRunner.setMapReduceIntegrater(new MapReduceIntegration() {
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

				final MemoryIndexStoreFactory factory = new MemoryIndexStoreFactory();
				try {
					Assert.assertTrue(factory.getIndexStore(
							configWrapper).indexExists(
							new ByteArrayId(
									"spatial")));

					final AdapterStoreFactory adapterStoreFactory = configWrapper.getInstance(
							CommonParameters.Common.ADAPTER_STORE_FACTORY,
							ConvexHullMapReduce.class,
							AdapterStoreFactory.class,
							AccumuloAdapterStoreFactory.class);

					Assert.assertEquals(
							FeatureDataAdapterStoreFactory.class,
							adapterStoreFactory.getClass());

					Assert.assertTrue(adapterStoreFactory.getAdapterStore(
							configWrapper).adapterExists(
							new ByteArrayId(
									"centroidtest")));

					final Projection<?> projection = configWrapper.getInstance(
							HullParameters.Hull.PROJECTION_CLASS,
							ConvexHullMapReduce.class,
							Projection.class,
							SimpleFeatureProjection.class);

					Assert.assertEquals(
							SimpleFeatureProjection.class,
							projection.getClass());

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
				Assert.assertEquals(
						2,
						configWrapper.getInt(
								CentroidParameters.Centroid.ZOOM_LEVEL,
								NestedGroupCentroidAssignment.class,
								-1));
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
		hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration());

		runTimeProperties.store(
				MRConfig.HDFS_BASE_DIR,
				"/");
		runTimeProperties.store(
				InputParameters.Input.HDFS_INPUT_PATH,
				new Path(
						"file://foo/bin"));
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
				"b1234");
		runTimeProperties.store(
				HullParameters.Hull.DATA_TYPE_ID,
				"hullType");
		runTimeProperties.store(
				HullParameters.Hull.REDUCER_COUNT,
				10);
		runTimeProperties.store(
				HullParameters.Hull.INDEX_ID,
				"spatial");
		runTimeProperties.store(
				CommonParameters.Common.ADAPTER_STORE_FACTORY,
				FeatureDataAdapterStoreFactory.class);

		runTimeProperties.store(
				CommonParameters.Common.INDEX_STORE_FACTORY,
				MemoryIndexStoreFactory.class);

		FeatureDataAdapterStoreFactory.saveState(
				new FeatureDataAdapter(
						ftype),
				runTimeProperties);
	}

	@Test
	public void testOptions() {
		final Set<Option> options = new HashSet<Option>();
		hullRunner.fillOptions(options);

		assertTrue(PropertyManagement.hasOption(
				options,
				CommonParameters.Common.ADAPTER_STORE_FACTORY));
		assertTrue(PropertyManagement.hasOption(
				options,
				CommonParameters.Common.INDEX_STORE_FACTORY));

		assertTrue(PropertyManagement.hasOption(
				options,
				InputParameters.Input.HDFS_INPUT_PATH));
		assertTrue(PropertyManagement.hasOption(
				options,
				HullParameters.Hull.WRAPPER_FACTORY_CLASS));
		assertTrue(PropertyManagement.hasOption(
				options,
				HullParameters.Hull.PROJECTION_CLASS));
		assertTrue(PropertyManagement.hasOption(
				options,
				HullParameters.Hull.REDUCER_COUNT));
		assertTrue(PropertyManagement.hasOption(
				options,
				HullParameters.Hull.DATA_TYPE_ID));
		assertTrue(PropertyManagement.hasOption(
				options,
				HullParameters.Hull.DATA_NAMESPACE_URI));
		assertTrue(PropertyManagement.hasOption(
				options,
				HullParameters.Hull.INDEX_ID));

		assertTrue(PropertyManagement.hasOption(
				options,
				MapReduceParameters.MRConfig.CONFIG_FILE));
		assertTrue(PropertyManagement.hasOption(
				options,
				MapReduceParameters.MRConfig.HDFS_HOST_PORT));
		assertTrue(PropertyManagement.hasOption(
				options,
				MapReduceParameters.MRConfig.HDFS_BASE_DIR));
		assertTrue(PropertyManagement.hasOption(
				options,
				MapReduceParameters.MRConfig.YARN_RESOURCE_MANAGER));
		assertTrue(PropertyManagement.hasOption(
				options,
				MapReduceParameters.MRConfig.JOBTRACKER_HOST_PORT));

		assertTrue(PropertyManagement.hasOption(
				options,
				CentroidParameters.Centroid.ZOOM_LEVEL));

		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.ZOOKEEKER));
		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.ACCUMULO_INSTANCE));
		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.ACCUMULO_USER));
		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.ACCUMULO_PASSWORD));
		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.BATCH_ID));
		assertTrue(PropertyManagement.hasOption(
				options,
				GlobalParameters.Global.ACCUMULO_NAMESPACE));
	}

	@Test
	public void test()
			throws Exception {

		hullRunner.run(runTimeProperties);
	}
}
