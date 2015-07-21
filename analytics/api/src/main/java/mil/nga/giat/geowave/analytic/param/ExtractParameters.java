package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.commons.cli.Option;

public class ExtractParameters
{
	public enum Extract
			implements
			ParameterEnum {
		OUTPUT_DATA_TYPE_ID(
				String.class),
		DATA_NAMESPACE_URI(
				String.class),
		REDUCER_COUNT(
				Integer.class),
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class),
		QUERY(
				DistributableQuery.class),
		QUERY_OPTIONS(
				QueryOptions.class),
		MAX_INPUT_SPLIT(
				Integer.class),
		MIN_INPUT_SPLIT(
				Integer.class),
		INDEX_ID(
				String.class),
		GROUP_ID(
				String.class),
		ADAPTER_ID(
				String.class);
		private final Class<?> baseClass;

		Extract(
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
			final Extract[] params ) {
		if (contains(
				params,
				Extract.OUTPUT_DATA_TYPE_ID)) {
			options.add(PropertyManagement.newOption(
					Extract.OUTPUT_DATA_TYPE_ID,
					"eot",
					"Output Data Type ID",
					true));
		}
		if (contains(
				params,
				Extract.ADAPTER_ID)) {
			options.add(PropertyManagement.newOption(
					Extract.ADAPTER_ID,
					"eit",
					"Input Data Type ID",
					true));
		}
		if (contains(
				params,
				Extract.MAX_INPUT_SPLIT)) {
			options.add(PropertyManagement.newOption(
					Extract.MAX_INPUT_SPLIT,
					"emx",
					"Maximum input split size",
					true));
		}
		if (contains(
				params,
				Extract.DATA_NAMESPACE_URI)) {
			options.add(PropertyManagement.newOption(
					Extract.DATA_NAMESPACE_URI,
					"ens",
					"Output Data Namespace URI",
					true));
		}
		if (contains(
				params,
				Extract.QUERY)) {
			options.add(PropertyManagement.newOption(
					Extract.QUERY,
					"eq",
					"Query",
					true));
		}
		if (contains(
				params,
				Extract.QUERY_OPTIONS)) {
			options.add(PropertyManagement.newOption(
					Extract.QUERY_OPTIONS,
					"eqf",
					"Restricted extracted field list (comma-separated list of field ids)",
					true));
		}
		if (contains(
				params,
				Extract.GROUP_ID)) {
			options.add(PropertyManagement.newOption(
					Extract.GROUP_ID,
					"eg",
					"Group ID assigned to extracted data",
					true));
		}
		if (contains(
				params,
				Extract.MIN_INPUT_SPLIT)) {
			options.add(PropertyManagement.newOption(
					Extract.MIN_INPUT_SPLIT,
					"emn",
					"Minimum input split size",
					true));
		}
		if (contains(
				params,
				Extract.REDUCER_COUNT)) {
			options.add(PropertyManagement.newOption(
					Extract.REDUCER_COUNT,
					"erc",
					"Number of Reducers For initial data extraction and de-duplication",
					true));
		}
		if (contains(
				params,
				Extract.DIMENSION_EXTRACT_CLASS)) {
			options.add(PropertyManagement.newOption(
					Extract.DIMENSION_EXTRACT_CLASS,
					"ede",
					"Class to extract dimensions into a simple feature output",
					true));
		}
	}

	private static boolean contains(
			final Extract[] params,
			final Extract option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
