package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.KMeansMapReduce;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opengis.feature.simple.SimpleFeature;

/**
 * 
 * Run 'K' means one time to move the centroids towards the mean.
 * 
 * 
 */
public class KMeansJobRunner extends
		GeoWaveAnalyticJobRunner implements
		MapReduceJobRunner
{

	public KMeansJobRunner() {
		super.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	@Override
	public void setReducerCount(
			final int reducerCount ) {
		super.setReducerCount(Math.min(
				2,
				reducerCount));
	}

	@Override
	public void configure(
			final Job job )
			throws Exception {
		job.setMapperClass(KMeansMapReduce.KMeansMapper.class);
		job.setMapOutputKeyClass(GroupIDText.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(KMeansMapReduce.KMeansReduce.class);
		job.setCombinerClass(KMeansMapReduce.KMeansCombiner.class);
		job.setReduceSpeculativeExecution(false);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(SimpleFeature.class);
	}

	@Override
	public Class<?> getScope() {
		return KMeansMapReduce.class;
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement runTimeProperties )
			throws Exception {
		NestedGroupCentroidAssignment.setParameters(
				configuration,
				runTimeProperties);
		super.setReducerCount(runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
				Math.max(
						2,
						super.getReducerCount())));

		RunnerUtils.setParameter(
				configuration,
				KMeansMapReduce.class,
				runTimeProperties,
				new ParameterEnum[] {
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
				});

		return super.run(
				configuration,
				runTimeProperties);
	}
}