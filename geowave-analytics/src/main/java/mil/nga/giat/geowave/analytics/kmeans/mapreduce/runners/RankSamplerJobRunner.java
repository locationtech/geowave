package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.sample.BahmanEtAlSampleProbabilityFn;
import mil.nga.giat.geowave.analytics.sample.functions.CentroidDistanceBasedSamplingRankFunction;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.hadoop.conf.Configuration;

/**
 * Sample K points given a sample function. The sampled K points are are stored
 * as centroids within GeoWave. The sampling weight may be determined by the
 * relation of a point to a current set of centroids, thus a {@link DistanceFn}
 * instance is required.
 * 
 */
public class RankSamplerJobRunner extends
		KSamplerJobRunner implements
		MapReduceJobRunner
{

	public RankSamplerJobRunner() {
		setSamplingRankFunctionClass(CentroidDistanceBasedSamplingRankFunction.class);
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		CentroidManagerGeoWave.setParameters(
				config,
				runTimeProperties);
		runTimeProperties.storeIfEmpty(
				SampleParameters.Sample.PROBABILITY_FUNCTION,
				BahmanEtAlSampleProbabilityFn.class);
		CentroidDistanceBasedSamplingRankFunction.setParameters(
				config,
				runTimeProperties);

		return super.run(
				config,
				runTimeProperties);
	}
}