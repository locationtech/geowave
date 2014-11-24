package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.UpdateCentroidCostMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.opengis.feature.simple.SimpleFeature;

/**
 * CentroidManagerGeoWave
 * 
 * 
 */
public class UpdateCentroidCostJobRunner extends
		GeoWaveJobRunner implements
		MapReduceJobRunner
{
	private Path inputHDFSPath = null;

	private int reducerCount = 1;

	public UpdateCentroidCostJobRunner() {

	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		CentroidManagerGeoWave.setParameters(
				config,
				runTimeProperties);

		NestedGroupCentroidAssignment.setParameters(
				config,
				runTimeProperties);

		RunnerUtils.setParameter(
				config,
				UpdateCentroidCostMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
				});

		return ToolRunner.run(
				config,
				this,
				runTimeProperties.toGeoWaveRunnerArguments());
	}

	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		this.inputHDFSPath = inputHDFSPath;
	}

	public void setReducerCount(
			final int reducerCount ) {
		this.reducerCount = reducerCount;
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {

		job.setJobName("GeoWave Update Centroid Cost (" + namespace + ")");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(UpdateCentroidCostMapReduce.UpdateCentroidCostMap.class);
		job.setMapOutputKeyClass(GroupIDText.class);
		job.setMapOutputValueClass(CountofDoubleWritable.class);
		job.setCombinerClass(UpdateCentroidCostMapReduce.UpdateCentroidCostCombiner.class);
		job.setReducerClass(UpdateCentroidCostMapReduce.UpdateCentroidCostReducer.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(SimpleFeature.class);
		job.setNumReduceTasks(reducerCount);

		GeoWaveConfiguratorBase.setZookeeperUrl(
				UpdateCentroidCostMapReduce.class,
				job,
				zookeeper);
		GeoWaveConfiguratorBase.setInstanceName(
				UpdateCentroidCostMapReduce.class,
				job,
				instance);
		GeoWaveConfiguratorBase.setUserName(
				UpdateCentroidCostMapReduce.class,
				job,
				user);
		GeoWaveConfiguratorBase.setPassword(
				UpdateCentroidCostMapReduce.class,
				job,
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				UpdateCentroidCostMapReduce.class,
				job,
				namespace);
		FileInputFormat.setInputPaths(
				job,
				inputHDFSPath);

	}
}