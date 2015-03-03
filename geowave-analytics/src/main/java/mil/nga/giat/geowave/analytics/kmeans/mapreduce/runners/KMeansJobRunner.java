package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.KMeansMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.opengis.feature.simple.SimpleFeature;

/**
 * 
 * Run 'K' means one time to move the centroids towards the mean.
 * 
 * 
 */
public class KMeansJobRunner extends
		GeoWaveJobRunner implements
		MapReduceJobRunner
{
	private Path inputHDFSPath = null;

	private int reducerCount = 2;

	public KMeansJobRunner() {}

	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		this.inputHDFSPath = inputHDFSPath;
	}

	public void setReducerCount(
			final int reducerCount ) {
		this.reducerCount = Math.min(
				2,
				reducerCount);
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setJobName("GeoWave K-Means (" + namespace + ")");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(KMeansMapReduce.KMeansMapper.class);
		job.setMapOutputKeyClass(GroupIDText.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(KMeansMapReduce.KMeansReduce.class);
		job.setCombinerClass(KMeansMapReduce.KMeansCombiner.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(SimpleFeature.class);
		job.setNumReduceTasks(reducerCount);

		// FOR INPUT DATA
		FileInputFormat.setInputPaths(
				job,
				inputHDFSPath);
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);
		reducerCount = Math.min(
				runTimeProperties.getPropertyAsInt(
						ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
						16),
				reducerCount);
		RunnerUtils.setParameter(
				config,
				KMeansMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
				});
		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}
}