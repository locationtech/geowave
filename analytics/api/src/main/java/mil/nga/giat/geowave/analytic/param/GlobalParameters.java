package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;

public class GlobalParameters
{
	public enum Global
			implements
			ParameterEnum {
		ZOOKEEKER(
				String.class),
		ACCUMULO_INSTANCE(
				String.class),
		ACCUMULO_USER(
				String.class),
		ACCUMULO_PASSWORD(
				String.class),
		ACCUMULO_NAMESPACE(
				String.class),
		PARENT_BATCH_ID(
				String.class),
		CRS_ID(
				String.class),
		BATCH_ID(
				String.class);
		private final Class<?> baseClass;

		Global(
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
			final Global[] params ) {
		if (contains(
				params,
				Global.ZOOKEEKER)) {
			options.add(PropertyManagement.newOption(
					Global.ZOOKEEKER,
					"z",
					"A comma-separated list of zookeeper servers used by an Accumulo instance.",
					true));
		}
		if (contains(
				params,
				Global.BATCH_ID)) {
			options.add(PropertyManagement.newOption(
					Global.BATCH_ID,
					"b",
					"Batch ID",
					true));
		}
		if (contains(
				params,
				Global.PARENT_BATCH_ID)) {
			options.add(PropertyManagement.newOption(
					Global.PARENT_BATCH_ID,
					"pb",
					"Batch ID",
					true));
		}
		if (contains(
				params,
				Global.CRS_ID)) {
			options.add(PropertyManagement.newOption(
					Global.CRS_ID,
					"crs",
					"CRS ID",
					true));
		}
		if (contains(
				params,
				Global.ACCUMULO_INSTANCE)) {
			options.add(PropertyManagement.newOption(
					Global.ACCUMULO_INSTANCE,
					"i",
					"The Accumulo instance ID",
					true));
			options.add(PropertyManagement.newOption(
					Global.ACCUMULO_USER,
					"u",
					"A valid Accumulo user ID",
					true));
			options.add(PropertyManagement.newOption(
					Global.ACCUMULO_PASSWORD,
					"p",
					"The password for the Accumulo user",
					true));
			options.add(PropertyManagement.newOption(
					Global.ACCUMULO_NAMESPACE,
					"n",
					"The table namespace (optional; default is no namespace)",
					true));
		}
	}

	private static boolean contains(
			final Global[] params,
			final Global option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
