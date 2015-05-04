package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.sample.SampleProbabilityFn;
import mil.nga.giat.geowave.analytic.sample.function.SamplingRankFunction;

import org.apache.commons.cli.Option;

public class SampleParameters
{
	public enum Sample
			implements
			ParameterEnum {
		SAMPLE_SIZE(
				Integer.class),
		MIN_SAMPLE_SIZE(
				Integer.class),
		MAX_SAMPLE_SIZE(
				Integer.class),
		DATA_TYPE_ID(
				String.class),
		INDEX_ID(
				String.class),
		SAMPLE_ITERATIONS(
				Integer.class),
		PROBABILITY_FUNCTION(
				SampleProbabilityFn.class),
		SAMPLE_RANK_FUNCTION(
				SamplingRankFunction.class);
		private final Class<?> baseClass;

		Sample(
				final Class<?> baseClass ) {
			this.baseClass = baseClass;
		}

		@Override
		public Class<?> getBaseClass() {
			return baseClass;
		}

		@Override
		public Enum<?> self() {
			return this;
		}
	}

	public static final void fillOptions(
			final Set<Option> options,
			final Sample[] params ) {
		if (contains(
				params,
				Sample.SAMPLE_ITERATIONS)) {
			options.add(PropertyManagement.newOption(
					Sample.SAMPLE_ITERATIONS,
					"ssi",
					"Minimum number of sample iterations",
					true));
		}
		if (contains(
				params,
				Sample.DATA_TYPE_ID)) {
			options.add(PropertyManagement.newOption(
					Sample.DATA_TYPE_ID,
					"sdt",
					"Sample Data Type Id",
					true));
		}

		if (contains(
				params,
				Sample.INDEX_ID)) {
			options.add(PropertyManagement.newOption(
					Sample.INDEX_ID,
					"sdt",
					"Sample Index Type Id",
					true));
		}
		if (contains(
				params,
				Sample.MIN_SAMPLE_SIZE)) {
			options.add(PropertyManagement.newOption(
					Sample.MIN_SAMPLE_SIZE,
					"sms",
					"Minimum Sample Size",
					true));
		}
		if (contains(
				params,
				Sample.MAX_SAMPLE_SIZE)) {
			options.add(PropertyManagement.newOption(
					Sample.MAX_SAMPLE_SIZE,
					"sxs",
					"Max Sample Size",
					true));
		}
		if (contains(
				params,
				Sample.SAMPLE_SIZE)) {
			options.add(PropertyManagement.newOption(
					Sample.SAMPLE_SIZE,
					"sss",
					"Sample Size",
					true));
		}
		if (contains(
				params,
				Sample.PROBABILITY_FUNCTION)) {
			options.add(PropertyManagement.newOption(
					Sample.PROBABILITY_FUNCTION,
					"spf",
					"The PDF determines the probability for samping an item. Used by specific sample rank functions, such as CentroidDistanceBasedSamplingRankFunction.",
					true));
		}
		if (contains(
				params,
				Sample.SAMPLE_RANK_FUNCTION)) {
			options.add(PropertyManagement.newOption(
					Sample.SAMPLE_RANK_FUNCTION,
					"srf",
					"The rank function used when sampling the first N highest rank items.",
					true));
		}
	}

	private static boolean contains(
			final Sample[] params,
			final Sample option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
