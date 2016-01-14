package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters.Output;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;

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

	public AdapterStore getAdapterStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return ((PersistableAdapterStore) StoreParam.ADAPTER_STORE.getHelper().getValue(
				runTimeProperties)).getCliOptions().createStore();
	}

	public IndexStore getIndexStore(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return ((PersistableIndexStore) StoreParam.INDEX_STORE.getHelper().getValue(
				runTimeProperties)).getCliOptions().createStore();
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement runTimeProperties )
			throws Exception {

		if ((inputFormat == null) && runTimeProperties.hasProperty(InputParameters.Input.INPUT_FORMAT)) {
			inputFormat = runTimeProperties.getClassInstance(
					InputParameters.Input.INPUT_FORMAT,
					FormatConfiguration.class,
					null);
		}
		if (inputFormat != null) {
			InputParameters.Input.INPUT_FORMAT.getHelper().setValue(
					configuration,
					getScope(),
					inputFormat.getClass());
			inputFormat.setup(
					runTimeProperties,
					configuration);
		}
		if ((outputFormat == null) && runTimeProperties.hasProperty(OutputParameters.Output.OUTPUT_FORMAT)) {
			outputFormat = runTimeProperties.getClassInstance(
					OutputParameters.Output.OUTPUT_FORMAT,
					FormatConfiguration.class,
					null);
		}

		if (outputFormat != null) {
			OutputParameters.Output.OUTPUT_FORMAT.getHelper().setValue(
					configuration,
					getScope(),
					outputFormat.getClass());
			outputFormat.setup(
					runTimeProperties,
					configuration);
		}

		runTimeProperties.setConfig(
				new ParameterEnum[] {
					StoreParam.ADAPTER_STORE,
					StoreParam.INDEX_STORE
				},
				configuration,
				getScope());

		OutputParameters.Output.REDUCER_COUNT.getHelper().setValue(
				configuration,
				getScope(),
				runTimeProperties.getPropertyAsInt(
						OutputParameters.Output.REDUCER_COUNT,
						reducerCount));
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
			final PrimaryIndex index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Job job = mapReduceIntegrater.getJob(this);

		configure(job);

		final ScopedJobConfiguration configWrapper = new ScopedJobConfiguration(
				job.getConfiguration(),
				getScope());

		final FormatConfiguration inputFormat = configWrapper.getInstance(
				InputParameters.Input.INPUT_FORMAT,
				FormatConfiguration.class,
				null);

		if (inputFormat != null) {
			job.setInputFormatClass((Class<? extends InputFormat>) inputFormat.getFormatClass());
		}

		final FormatConfiguration outputFormat = configWrapper.getInstance(
				OutputParameters.Output.OUTPUT_FORMAT,
				FormatConfiguration.class,
				null);

		if (outputFormat != null) {
			job.setOutputFormatClass((Class<? extends OutputFormat>) outputFormat.getFormatClass());
		}

		job.setNumReduceTasks(configWrapper.getInt(
				OutputParameters.Output.REDUCER_COUNT,
				1));

		job.setJobName(getJobName());

		job.setJarByClass(this.getClass());
		final Counters counters = mapReduceIntegrater.waitForCompletion(job);
		lastCounterSet = counters;
		return (counters == null) ? 1 : 0;
	}

	abstract protected String getJobName();

	public long getCounterValue(
			final Enum<?> counterEnum ) {
		return (lastCounterSet != null) ? (lastCounterSet.findCounter(counterEnum)).getValue() : 0;
	}

	public abstract void configure(
			final Job job )
			throws Exception;

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		final List<ParameterEnum<?>> params = new ArrayList<ParameterEnum<?>>();
		if (inputFormat != null) {
			params.addAll(inputFormat.getParameters());
		}
		if (outputFormat != null) {
			params.addAll(inputFormat.getParameters());
		}
		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			StoreParam.ADAPTER_STORE,
			StoreParam.INDEX_STORE,
			Output.REDUCER_COUNT,
			Output.OUTPUT_FORMAT
		}));
		return params;
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				mapReduceIntegrater.getConfiguration(runTimeProperties),
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

		PrimaryIndex index = (PrimaryIndex) indexStore.getIndex(new ByteArrayId(
				indexId));
		if (index == null) {
			final PrimaryIndex defaultSpatialIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
			index = new CustomIdIndex(
					defaultSpatialIndex.getIndexStrategy(),
					defaultSpatialIndex.getIndexModel(),
					new ByteArrayId(
							indexId));
			indexStore.addIndex(index);
		}
		return indexId;
	}

}
