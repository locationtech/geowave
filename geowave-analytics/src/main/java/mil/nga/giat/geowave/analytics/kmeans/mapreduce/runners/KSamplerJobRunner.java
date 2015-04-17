package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import java.util.UUID;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.KSamplerMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.sample.functions.RandomSamplingRankFunction;
import mil.nga.giat.geowave.analytics.sample.functions.SamplingRankFunction;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.FormatConfiguration;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * Samples 'K' number of data items by evaluating a {@link SamplingRankFunction}
 * 
 * For KMeans Parallel, the initial step requires seeding the centroids with a
 * single point. In this case, K=1 and the rank function is random. This means
 * the top selected geometry is random. In addition, each subsequent iteration
 * samples based on probability function and K is some provided sample size.
 * 
 * 
 */
public class KSamplerJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner
{
	protected int zoomLevel = 1;
	private Class<? extends SamplingRankFunction> samplingRankFunctionClass = RandomSamplingRankFunction.class;

	public KSamplerJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setSamplingRankFunctionClass(
			final Class<? extends SamplingRankFunction> samplingRankFunctionClass ) {
		this.samplingRankFunctionClass = samplingRankFunctionClass;
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public Class<?> getScope() {
		return KSamplerMapReduce.class;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(KSamplerMapReduce.SampleMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(KSamplerMapReduce.SampleReducer.class);
		job.setPartitionerClass(KSamplerMapReduce.SampleKeyPartitioner.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
	}

	private DataAdapter<?> getAdapter(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final AdapterStore adapterStore = super.getAdapterStore(runTimeProperties);

		return adapterStore.getAdapter(new ByteArrayId(
				runTimeProperties.getPropertyAsString(
						SampleParameters.Sample.DATA_TYPE_ID,
						"sample")));
	}

	private Index getIndex(
			final PropertyManagement runTimeProperties )
			throws Exception {
		final IndexStore indexStore = super.getIndexStore(runTimeProperties);

		return indexStore.getIndex(new ByteArrayId(
				runTimeProperties.getPropertyAsString(
						SampleParameters.Sample.INDEX_ID,
						"index")));
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		runTimeProperties.storeIfEmpty(
				SampleParameters.Sample.DATA_TYPE_ID,
				"sample");

		runTimeProperties.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				zoomLevel);

		runTimeProperties.storeIfEmpty(
				SampleParameters.Sample.INDEX_ID,
				IndexType.SPATIAL_TEMPORAL_VECTOR.getDefaultId());
		RunnerUtils.setParameter(
				config,
				KSamplerMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					GlobalParameters.Global.BATCH_ID,
					SampleParameters.Sample.INDEX_ID,
					SampleParameters.Sample.SAMPLE_SIZE,
					SampleParameters.Sample.DATA_TYPE_ID,
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidParameters.Centroid.ZOOM_LEVEL
				});

		RunnerUtils.setParameter(
				config,
				KSamplerMapReduce.class,
				new Object[] {
					samplingRankFunctionClass
				},
				new ParameterEnum[] {
					SampleParameters.Sample.SAMPLE_RANK_FUNCTION,
				});

		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);

		addDataAdapter(
				config,
				getAdapter(runTimeProperties));
		addIndex(
				config,
				getIndex(runTimeProperties));

		super.setReducerCount(zoomLevel);
		return super.run(
				config,
				runTimeProperties);

	}

}