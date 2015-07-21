package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.db.AccumuloAdapterStoreFactory;
import mil.nga.giat.geowave.analytic.db.AccumuloIndexStoreFactory;
import mil.nga.giat.geowave.analytic.db.AdapterStoreFactory;
import mil.nga.giat.geowave.analytic.db.IndexStoreFactory;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters.Output;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.geotime.DimensionalityType;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextIndexStore;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Tool;
import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class managers the input and output formats for a map reduce job. It
 * also controls job submission, isolating some of the job management
 * responsibilities. One key benefit is support of unit testing for job runner
 * instances.
 */
public abstract class GeoWaveAnalyticJobRunner extends
		Configured implements
		Tool,
		MapReduceJobRunner,
		IndependentJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveAnalyticJobRunner.class);

	private FormatConfiguration inputFormat = null;
	private FormatConfiguration outputFormat = null;
	private int reducerCount = 1;
	private MapReduceIntegration mapReduceIntegrater = new ToolRunnerMapReduceIntegration();
	private Counters lastCounterSet = null;

	/**
	 * Data Store Parameters
	 */
	protected String zookeeper, instanceName, userName, password, namespace;

	public FormatConfiguration getInputFormatConfiguration() {
		return inputFormat;
	}

	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormat ) {
		this.inputFormat = inputFormat;
	}

	public FormatConfiguration getOutputFormatConfiguration() {
		return outputFormat;
	}

	public void setOutputFormatConfiguration(
			final FormatConfiguration outputFormat ) {
		this.outputFormat = outputFormat;
	}

	public MapReduceIntegration getMapReduceIntegrater() {
		return mapReduceIntegrater;
	}

	public void setMapReduceIntegrater(
			final MapReduceIntegration mapReduceIntegrater ) {
		this.mapReduceIntegrater = mapReduceIntegrater;
	}

	public int getReducerCount() {
		return reducerCount;
	}

	public void setReducerCount(
			final int reducerCount ) {
		this.reducerCount = reducerCount;
	}

	public GeoWaveAnalyticJobRunner() {}

	protected static Logger getLogger() {
		return LOGGER;
	}

	public Class<?> getScope() {
		return this.getClass();
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement runTimeProperties )
			throws Exception {

		if (inputFormat == null && runTimeProperties.hasProperty(InputParameters.Input.INPUT_FORMAT)) {
			inputFormat = runTimeProperties.getClassInstance(
					InputParameters.Input.INPUT_FORMAT,
					FormatConfiguration.class,
					null);
		}
		if (inputFormat != null) {
			RunnerUtils.setParameter(
					configuration,
					getScope(),
					new Object[] {
						inputFormat.getClass()
					},
					new ParameterEnum[] {
						InputParameters.Input.INPUT_FORMAT
					});
			inputFormat.setup(
					runTimeProperties,
					configuration);
		}
		if (outputFormat == null && runTimeProperties.hasProperty(OutputParameters.Output.OUTPUT_FORMAT)) {
			outputFormat = runTimeProperties.getClassInstance(
					OutputParameters.Output.OUTPUT_FORMAT,
					FormatConfiguration.class,
					null);
		}

		if (outputFormat != null) {
			RunnerUtils.setParameter(
					configuration,
					getScope(),
					new Object[] {
						outputFormat.getClass()
					},
					new ParameterEnum[] {
						OutputParameters.Output.OUTPUT_FORMAT
					});
			outputFormat.setup(
					runTimeProperties,
					configuration);
		}

		RunnerUtils.setParameter(
				configuration,
				getScope(),
				runTimeProperties,
				new ParameterEnum[] {
					CommonParameters.Common.ADAPTER_STORE_FACTORY,
					CommonParameters.Common.INDEX_STORE_FACTORY
				});

		RunnerUtils.setParameter(
				configuration,
				getScope(),
				new Object[] {
					runTimeProperties.getPropertyAsInt(
							OutputParameters.Output.REDUCER_COUNT,
							reducerCount)
				},
				new ParameterEnum[] {
					OutputParameters.Output.REDUCER_COUNT
				});
		return mapReduceIntegrater.submit(
				configuration,
				runTimeProperties,
				this);
	}

	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {
		JobContextAdapterStore.addDataAdapter(
				config,
				adapter);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	public AdapterStore getAdapterStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return runTimeProperties.getClassInstance(
				CommonParameters.Common.ADAPTER_STORE_FACTORY,
				AdapterStoreFactory.class,
				AccumuloAdapterStoreFactory.class).getAdapterStore(
				runTimeProperties);
	}

	public IndexStore getIndexStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return runTimeProperties.getClassInstance(
				CommonParameters.Common.INDEX_STORE_FACTORY,
				IndexStoreFactory.class,
				AccumuloIndexStoreFactory.class).getIndexStore(
				runTimeProperties);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Job job = mapReduceIntegrater.getJob(this);

		zookeeper = args[0];
		instanceName = args[1];
		userName = args[2];
		password = args[3];
		namespace = args[4];

		configure(job);

		final JobContextConfigurationWrapper configWrapper = new JobContextConfigurationWrapper(
				job);

		final FormatConfiguration inputFormat = configWrapper.getInstance(
				InputParameters.Input.INPUT_FORMAT,
				getScope(),
				FormatConfiguration.class,
				null);

		if (inputFormat != null) {
			job.setInputFormatClass((Class<? extends InputFormat>) inputFormat.getFormatClass());
		}

		final FormatConfiguration outputFormat = configWrapper.getInstance(
				OutputParameters.Output.OUTPUT_FORMAT,
				getScope(),
				FormatConfiguration.class,
				null);

		if (outputFormat != null) {
			job.setOutputFormatClass((Class<? extends OutputFormat>) outputFormat.getFormatClass());
		}

		job.setJobName("GeoWave Convex Hull (" + namespace + ")");

		job.setNumReduceTasks(configWrapper.getInt(
				OutputParameters.Output.REDUCER_COUNT,
				getScope(),
				1));

		job.setJarByClass(this.getClass());
		final Counters counters = mapReduceIntegrater.waitForCompletion(job);
		lastCounterSet = counters;
		return (counters == null) ? 1 : 0;
	}

	public long getCounterValue(
			final Enum<?> counterEnum ) {
		return (lastCounterSet != null) ? (lastCounterSet.findCounter(counterEnum)).getValue() : 0;
	}

	public abstract void configure(
			final Job job )
			throws Exception;

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		if (inputFormat != null) {
			inputFormat.fillOptions(options);
		}
		if (outputFormat != null) {
			outputFormat.fillOptions(options);
		}
		OutputParameters.fillOptions(
				options,
				new OutputParameters.Output[] {
					OutputParameters.Output.REDUCER_COUNT,
					OutputParameters.Output.OUTPUT_FORMAT
				});

		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.ADAPTER_STORE_FACTORY,
					CommonParameters.Common.INDEX_STORE_FACTORY
				});
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

	protected DataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties,
			final ParameterEnum dataTypeEnum,
			final ParameterEnum dataNameSpaceEnum )
			throws Exception {

		final String projectionDataTypeId = runTimeProperties.storeIfEmpty(
				dataTypeEnum,
				"convex_hull").toString();

		final AdapterStore adapterStore = getAdapterStore(runTimeProperties);

		DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				projectionDataTypeId));

		if (adapter == null) {
			final String namespaceURI = runTimeProperties.storeIfEmpty(
					dataNameSpaceEnum,
					BasicFeatureTypes.DEFAULT_NAMESPACE).toString();
			adapter = AnalyticFeature.createGeometryFeatureAdapter(
					projectionDataTypeId,
					new String[0],
					namespaceURI,
					ClusteringUtils.CLUSTERING_CRS);

			adapterStore.addAdapter(adapter);
		}
		return adapter;
	}

	protected String checkIndex(
			final PropertyManagement runTimeProperties,
			final ParameterEnum indexIdEnum,
			final String defaultIdxName )
			throws Exception {

		final String indexId = runTimeProperties.getPropertyAsString(
				indexIdEnum,
				defaultIdxName);

		final IndexStore indexStore = getIndexStore(runTimeProperties);

		Index index = indexStore.getIndex(new ByteArrayId(
				indexId));
		if (index == null) {
			index = new CustomIdIndex(
					IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy(),
					IndexType.SPATIAL_VECTOR.getDefaultIndexModel(),
					new ByteArrayId(
							indexId));
			indexStore.addIndex(index);
		}
		return indexId;
	}

}
