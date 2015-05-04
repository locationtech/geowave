package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.sample.BahmanEtAlSampleProbabilityFn;
import mil.nga.giat.geowave.analytic.sample.function.CentroidDistanceBasedSamplingRankFunction;

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