package mil.nga.giat.geowave.analytic.param;

public class ClusteringParameters
{

	public enum Clustering
			implements
			ParameterEnum {
		MAX_REDUCER_COUNT(
				Integer.class,
				"crc",
				"Maximum Clustering Reducer Count",
				true),
		RETAIN_GROUP_ASSIGNMENTS(
				Boolean.class,
				"ga",
				"Retain Group assignments during execution",
				false),
		MINIMUM_SIZE(
				Integer.class,
				"cms",
				"Minimum Cluster Size",
				true),
		MAX_ITERATIONS(
				Integer.class,
				"cmi",
				"Maximum number of iterations when finding optimal clusters",
				true),
		CONVERGANCE_TOLERANCE(
				Double.class,
				"cct",
				"Convergence Tolerance",
				true),
		DISTANCE_THRESHOLDS(
				String.class,
				"dt",
				"Comma separated list of distance thresholds, per dimension",
				true),
		GEOMETRIC_DISTANCE_UNIT(
				String.class,
				"du",
				"Geometric distance unit (m=meters,km=kilometers, see symbols for javax.units.BaseUnit)",
				true),
		ZOOM_LEVELS(
				Integer.class,
				"zl",
				"Number of Zoom Levels to Process",
				true);

		private final ParameterHelper<?> helper;

		private Clustering(
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
