package mil.nga.giat.geowave.analytics.clustering.runners;

import java.util.Set;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.clustering.mapreduce.GroupAssignmentMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Assign group IDs to input items based on centroids.
 * 
 * 
 */
public class GroupAssigmentJobRunner extends
		GeoWaveAnalyticJobRunner
{
	private int zoomLevel = 1;

	public GroupAssigmentJobRunner() {
		super.setReducerCount(8);
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public void configure(
			Job job )
			throws Exception {
		job.setMapperClass(GroupAssignmentMapReduce.GroupAssignmentMapper.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(ObjectWritable.class);
	}

	@Override
	public Class<?> getScope() {
		return GroupAssignmentMapReduce.class;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		// Required since the Mapper uses the input format parameters to lookup
		// the adapter
		GeoWaveInputFormat.setAccumuloOperationsInfo(
				config,
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ZOOKEEKER,
						"localhost:2181"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_INSTANCE,
						"miniInstance"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_USER,
						"root"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_PASSWORD,
						"password"),
				runTimeProperties.getPropertyAsString(
						GlobalParameters.Global.ACCUMULO_NAMESPACE,
						"undefined"));

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

		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				zoomLevel);

		return super.run(
				config,
				runTimeProperties);
	}

	@Override
	public void fillOptions(
			Set<Option> options ) {
		super.fillOptions(options);

		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
					GlobalParameters.Global.BATCH_ID
				});

		CentroidManagerGeoWave.fillOptions(options);
		MapReduceParameters.fillOptions(options);
		NestedGroupCentroidAssignment.fillOptions(options);

	}

}