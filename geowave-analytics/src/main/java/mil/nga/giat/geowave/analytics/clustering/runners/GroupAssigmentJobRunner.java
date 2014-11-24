package mil.nga.giat.geowave.analytics.clustering.runners;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.clustering.mapreduce.GroupAssignmentMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Assign group IDs to input items based on centroids.
 * 
 * 
 */
public class GroupAssigmentJobRunner extends
		Configured implements
		Tool,
		MapReduceJobRunner
{
	private int zoomLevel = 1;
	private int reducerCount = 8;
	private Path outputHDFSPath;
	private Path inputHDFSPath;

	public GroupAssigmentJobRunner() {

	}

	public void setOutputHDFSPath(
			final Path outputHDFSPath ) {
		this.outputHDFSPath = outputHDFSPath;
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	public void setReducerCount(
			final int reducerCount ) {
		this.reducerCount = reducerCount;
	}

	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		this.inputHDFSPath = inputHDFSPath;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Configuration conf = super.getConf();
		final Job job = new Job(
				conf);
		final String zookeeper = args[0];
		final String instance = args[1];
		final String user = args[2];
		final String password = args[3];
		final String namespace = args[4];

		job.setJarByClass(this.getClass());

		job.setJobName("GeoWave Group Assignment(" + namespace + ")");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(GroupAssignmentMapReduce.GroupAssignmentMapper.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(ObjectWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(reducerCount);

		NestedGroupCentroidAssignment.setZoomLevel(
				conf,
				zoomLevel);

		FileInputFormat.setInputPaths(
				job,
				inputHDFSPath);

		FileOutputFormat.setOutputPath(
				job,
				outputHDFSPath);

		// Required since the Mapper uses the input format parameters to lookup
		// the adapter
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		RunnerUtils.setParameter(
				config,
				GroupAssignmentMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				});
		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);
		CentroidManagerGeoWave.setParameters(
				config,
				runTimeProperties);
		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}
}