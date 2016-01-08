package mil.nga.giat.geowave.analytic.param;

import org.apache.hadoop.fs.Path;

public class OutputParameters
{
	public enum Output
			implements
			ParameterEnum<Object> {
		REDUCER_COUNT(
				Integer.class,
				"orc",
				"Number of Reducers For Output",
				true),
		OUTPUT_FORMAT(
				FormatConfiguration.class,
				"ofc",
				"Output Format Class",
				true),
		INDEX_ID(
				String.class,
				"oid",
				"Output Index ID for objects that will be written to GeoWave",
				true),
		DATA_TYPE_ID(
				String.class,
				"odt",
				"Output Data ID assigned to objects that will be written to GeoWave",
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"ons",
				"Output namespace for objects that will be written to GeoWave",
				true),
		HDFS_OUTPUT_PATH(
				Path.class,
				"oop",
				"Output HDFS File Path",
				true);
		private final ParameterHelper<Object> helper;

		private Output(
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
