package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

public class OutputParameters
{
	public enum Output
			implements
			ParameterEnum {
		REDUCER_COUNT(
				Integer.class),
		OUTPUT_FORMAT(
				FormatConfiguration.class),
		INDEX_ID(
				String.class),
		DATA_TYPE_ID(
				String.class),
		DATA_NAMESPACE_URI(
				String.class),
		HDFS_OUTPUT_PATH(
				Path.class);
		private final Class<?> baseClass;

		Output(
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
			final Output[] params ) {

		if (contains(
				params,
				Output.HDFS_OUTPUT_PATH)) {
			options.add(PropertyManagement.newOption(
					Output.HDFS_OUTPUT_PATH,
					"oop",
					"Output HDFS File Path",
					true));
		}
		if (contains(
				params,
				Output.REDUCER_COUNT)) {
			options.add(PropertyManagement.newOption(
					Output.REDUCER_COUNT,
					"orc",
					"Number of Reducers For Output",
					true));
		}
		if (contains(
				params,
				Output.INDEX_ID)) {
			options.add(PropertyManagement.newOption(
					Output.INDEX_ID,
					"oid",
					"Output Index ID assigned to GeoWave destined objects",
					true));
		}
		if (contains(
				params,
				Output.DATA_TYPE_ID)) {
			options.add(PropertyManagement.newOption(
					Output.DATA_TYPE_ID,
					"odt",
					"Output Data ID assigned to GeoWave destined objects",
					true));
		}
		if (contains(
				params,
				Output.DATA_NAMESPACE_URI)) {
			options.add(PropertyManagement.newOption(
					Output.DATA_NAMESPACE_URI,
					"ons",
					"Output Data ID assigned to GeoWave destined objects",
					true));
		}
		if (contains(
				params,
				Output.OUTPUT_FORMAT)) {
			options.add(PropertyManagement.newOption(
					Output.OUTPUT_FORMAT,
					"ofc",
					"Output Format Class",
					true));
		}
	}

	private static boolean contains(
			final Output[] params,
			final Output option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
