package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.apache.commons.cli.Option;

public class JumpParameters
{
	public enum Jump
			implements
			ParameterEnum {
		RANGE_OF_CENTROIDS(
				NumericRange.class),
		KPLUSPLUS_MIN(
				Integer.class),
		COUNT_OF_CENTROIDS(
				Integer.class);

		private final Class<?> baseClass;

		Jump(
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
			final Jump[] params ) {
		if (contains(
				params,
				Jump.RANGE_OF_CENTROIDS)) {
			options.add(PropertyManagement.newOption(
					Jump.RANGE_OF_CENTROIDS,
					"jrc",
					"Comma-separated range of centroids (e.g. 2,100)",
					true));
		}
		if (contains(
				params,
				Jump.KPLUSPLUS_MIN)) {
			options.add(PropertyManagement.newOption(
					Jump.KPLUSPLUS_MIN,
					"jkp",
					"The minimum k when K means ++ takes over sampling.",
					true));
		}
	}

	private static boolean contains(
			final Jump[] params,
			final Jump option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
