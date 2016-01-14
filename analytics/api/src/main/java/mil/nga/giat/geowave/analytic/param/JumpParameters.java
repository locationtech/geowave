package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

public class JumpParameters
{
	public enum Jump
			implements
			ParameterEnum {
		RANGE_OF_CENTROIDS(
				NumericRange.class,
				"jrc",
				"Comma-separated range of centroids (e.g. 2,100)",
				true),
		KPLUSPLUS_MIN(
				Integer.class,
				"jkp",
				"The minimum k when K means ++ takes over sampling.",
				true),
		COUNT_OF_CENTROIDS(
				Integer.class,
				"jcc",
				"Set the count of centroids for one run of kmeans.",
				true);

		private final ParameterHelper<?> helper;

		private Jump(
				final Class baseClass,
				final String name,
				final String description,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					hasArg);
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public ParameterHelper<?> getHelper() {
			return helper;
		}
	}
}
