package mil.nga.giat.geowave.analytic.sample.function;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;

/**
 * Used by {@link KSamplerMapReduce} to rank an object for selection in the
 * sample set. The top k highest ranked objects are sampled. Rank is between 0.0
 * and 1.0 inclusive.
 * 
 */
public interface SamplingRankFunction<T>
{
	public void initialize(
			final JobContext context,
			Class<?> scope,
			Logger logger )
			throws IOException;

	public double rank(
			final int sampleSize,
			T value );
}
