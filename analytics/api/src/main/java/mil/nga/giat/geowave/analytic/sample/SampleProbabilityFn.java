package mil.nga.giat.geowave.analytic.sample;

public interface SampleProbabilityFn
{
	public boolean requiresConstant();

	public double getProbability(
			double weight,
			double normalizingConstant,
			int sampleSize );
}
