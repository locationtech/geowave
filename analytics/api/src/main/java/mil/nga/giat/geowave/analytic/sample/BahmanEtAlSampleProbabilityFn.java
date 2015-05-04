package mil.nga.giat.geowave.analytic.sample;

/**
 * l * d^2(y,C)/phi_x(C) y is some point, C is a set of centroids and l is an
 * oversampling factor. As documented in section 3.3 in
 * 
 * Bahmani, Kumar, Moseley, Vassilvitskii and Vattani. Scalable K-means++. VLDB
 * Endowment Vol. 5, No. 7. 2012.
 */

public class BahmanEtAlSampleProbabilityFn implements
		SampleProbabilityFn
{

	@Override
	public double getProbability(
			final double weight,
			final double normalizingConstant,
			final int sampleSize ) {
		return (((double) sampleSize) * weight) / normalizingConstant;
	}

	@Override
	public boolean requiresConstant() {
		return true;
	}

}
