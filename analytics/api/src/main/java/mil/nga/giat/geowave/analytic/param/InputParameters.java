package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;

public class InputParameters
{
	public enum Input
			implements
			ParameterEnum {
		INPUT_FORMAT(
				FormatConfiguration.class),
		HDFS_INPUT_PATH(
				Path.class);
		private final Class<?> baseClass;

		Input(
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
			final Input[] params ) {

		if (contains(
				params,
				Input.HDFS_INPUT_PATH)) {
			options.add(PropertyManagement.newOption(
					Input.HDFS_INPUT_PATH,
					"iip",
					"Input HDFS File Path",
					true));
		}

		if (contains(
				params,
				Input.INPUT_FORMAT)) {
			options.add(PropertyManagement.newOption(
					Input.INPUT_FORMAT,
					"ifc",
					"Input Format Class",
					true));
		}

	}

	private static boolean contains(
			final Input[] params,
			final Input option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
