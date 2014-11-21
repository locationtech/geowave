package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import java.util.UUID;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
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
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;

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
		GeoWaveJobRunner implements
		MapReduceJobRunner
{
	protected Path inputHDFSPath;
	protected int zoomLevel = 1;
	private Class<? extends SamplingRankFunction> samplingRankFunctionClass = RandomSamplingRankFunction.class;
	private String sampleDataTypeId;
	private String indexId;

	public KSamplerJobRunner() {

	}

	public void setSamplingRankFunctionClass(
			final Class<? extends SamplingRankFunction> samplingRankFunctionClass ) {
		this.samplingRankFunctionClass = samplingRankFunctionClass;
	}

	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		this.inputHDFSPath = inputHDFSPath;
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setJobName("GeoWave K-Sample (" + namespace + ")");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(KSamplerMapReduce.SampleMap.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(KSamplerMapReduce.SampleReducer.class);
		job.setPartitionerClass(KSamplerMapReduce.SampleKeyPartitioner.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
		job.setNumReduceTasks(zoomLevel); // increase the tasks for each level
											// to compensate for the increased
											// work load

		RunnerUtils.setParameter(
				job.getConfiguration(),
				KSamplerMapReduce.class,
				new Object[] {
					samplingRankFunctionClass
				},
				new ParameterEnum[] {
					SampleParameters.Sample.SAMPLE_RANK_FUNCTION,
				});

		FileInputFormat.setInputPaths(
				job,
				inputHDFSPath);

		addDataAdapter(getAdapter(job));
		addIndex(getIndex());
	}

	private DataAdapter<?> getAdapter(
			final Job job )
			throws Exception {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		return adapterStore.getAdapter(new ByteArrayId(
				sampleDataTypeId));
	}

	private Index getIndex()
			throws Exception {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				instance,
				user,
				password,
				namespace);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);

		return indexStore.getIndex(new ByteArrayId(
				indexId));
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
		
		runTimeProperties.storeIfEmpty(
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

		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);

		sampleDataTypeId = runTimeProperties.getProperty(SampleParameters.Sample.DATA_TYPE_ID);
		indexId = runTimeProperties.getProperty(SampleParameters.Sample.INDEX_ID);

		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}

}