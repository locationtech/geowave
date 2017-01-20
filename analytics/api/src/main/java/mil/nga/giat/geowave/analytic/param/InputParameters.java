package mil.nga.giat.geowave.analytic.param;

import org.apache.hadoop.fs.Path;

public class InputParameters
{
	public enum Input
			implements
			ParameterEnum<Object> {
		INPUT_FORMAT(
				FormatConfiguration.class,
				"ifc",
				"Input Format Class",
				true,
				true),
		HDFS_INPUT_PATH(
				Path.class,
				"iip",
				"Input HDFS File Path",
				false,
				true);

		private final ParameterHelper<Object> helper;

		private Input(
				final Class baseClass,
				final String name,
				final String description,
				final boolean isClass,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					isClass,
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
