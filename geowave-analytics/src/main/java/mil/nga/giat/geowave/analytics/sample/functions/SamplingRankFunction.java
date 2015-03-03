package mil.nga.giat.geowave.analytics.sample.functions;

import java.io.IOException;

import mil.nga.giat.geowave.analytics.kmeans.mapreduce.KSamplerMapReduce;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;

/**
 * Used by {@link KSamplerMapReduce} to rank an object for selection in the
 * sample set. The top k highest ranked objects are sampled. Rank is between 0.0
 * and 1.0 inclusive.
 * 
 */
public interface SamplingRankFunction<T>
{
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException;

	public double rank(
			final int sampleSize,
			T value );
}
