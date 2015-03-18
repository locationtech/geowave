package mil.nga.giat.geowave.analytics.sample;

public interface SampleProbabilityFn
{
	public boolean requiresConstant();

	public double getProbability(
			double weight,
			double normalizingConstant,
			int sampleSize );
}
