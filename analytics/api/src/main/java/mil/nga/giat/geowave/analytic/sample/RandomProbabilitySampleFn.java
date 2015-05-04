package mil.nga.giat.geowave.analytic.sample;

import java.util.Random;

public class RandomProbabilitySampleFn implements
		SampleProbabilityFn
{
	final Random random = new Random();

	@Override
	public double getProbability(
			final double weight,
			final double normalizingConstant,
			final int sampleSize ) {
		return Math.log(random.nextDouble()) / (weight / normalizingConstant);
	}

	@Override
	public boolean requiresConstant() {
		return false;
	}

}
