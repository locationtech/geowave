package mil.nga.giat.geowave.analytic.param;

public class GlobalParameters
{
	public enum Global
			implements
			ParameterEnum<Object> {
		PARENT_BATCH_ID(
				String.class,
				"pb",
				"Batch ID",
				true),
		CRS_ID(
				String.class,
				"crs",
				"CRS ID",
				true),
		BATCH_ID(
				String.class,
				"b",
				"Batch ID",
				true);
		private final ParameterHelper<Object> helper;

		private Global(
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
		public ParameterHelper<Object> getHelper() {
			return helper;
		}
	}
}
