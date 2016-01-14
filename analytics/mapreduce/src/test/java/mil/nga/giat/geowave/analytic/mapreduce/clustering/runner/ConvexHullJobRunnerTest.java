package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.io.IOException;
import java.util.HashMap;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.ConvexHullMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.param.ParameterHelper;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryIndexStoreFactory;

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
	private final ConvexHullJobRunner hullRunner = new ConvexHullJobRunner();
	private final PropertyManagement runTimeProperties = new PropertyManagement();
	private static final String TEST_NAMESPACE = "test";

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
				((ParameterHelper<Object>) StoreParam.ADAPTER_STORE.getHelper()).setValue(
						configuration,
						ConvexHullMapReduce.class,
						StoreParam.ADAPTER_STORE.getHelper().getValue(
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
						ConvexHullMapReduce.class);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));
				final PersistableIndexStore persistableIndexStore = (PersistableIndexStore) StoreParam.INDEX_STORE.getHelper().getValue(
						job,
						ConvexHullMapReduce.class,
						null);
				final IndexStore indexStore = persistableIndexStore.getCliOptions().createStore();
				try {
					Assert.assertTrue(indexStore.indexExists(new ByteArrayId(
							"spatial")));

					final PersistableAdapterStore persistableAdapterStore = (PersistableAdapterStore) StoreParam.ADAPTER_STORE.getHelper().getValue(
							job,
							ConvexHullMapReduce.class,
							null);
					final AdapterStore adapterStore = persistableAdapterStore.getCliOptions().createStore();

					Assert.assertTrue(adapterStore.adapterExists(new ByteArrayId(
							"centroidtest")));

					final Projection<?> projection = configWrapper.getInstance(
							HullParameters.Hull.PROJECTION_CLASS,
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

			@Override
			public Configuration getConfiguration(
					final PropertyManagement runTimeProperties )
					throws IOException {
				return new Configuration();
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
				StoreParam.DATA_STORE,
				new PersistableDataStore(
						new DataStoreCommandLineOptions(
								new MemoryDataStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));
		final MemoryAdapterStoreFactory adapterStoreFactory = new MemoryAdapterStoreFactory();
		runTimeProperties.store(
				StoreParam.ADAPTER_STORE,
				new PersistableAdapterStore(
						new AdapterStoreCommandLineOptions(
								adapterStoreFactory,
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));

		runTimeProperties.store(
				StoreParam.INDEX_STORE,
				new PersistableIndexStore(
						new IndexStoreCommandLineOptions(
								new MemoryIndexStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));
		adapterStoreFactory.createStore(
				new HashMap<String, Object>(),
				TEST_NAMESPACE).addAdapter(
				new FeatureDataAdapter(
						ftype));
	}

	@Test
	public void test()
			throws Exception {

		hullRunner.run(runTimeProperties);
	}
}
