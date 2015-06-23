package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;

import org.apache.commons.cli.Option;

public class HullParameters
{
	public enum Hull
			implements
			ParameterEnum {
		INDEX_ID(
				String.class),
		DATA_TYPE_ID(
				String.class),
		DATA_NAMESPACE_URI(
				String.class),
		REDUCER_COUNT(
				Integer.class),
		PROJECTION_CLASS(
				Projection.class),
		EXTRACTOR_CLASS(
				CentroidExtractor.class),
		WRAPPER_FACTORY_CLASS(
				AnalyticItemWrapperFactory.class),
		ITERATION(
				Integer.class),
		HULL_BUILDER(
				Projection.class),
		ZOOM_LEVEL(
				Integer.class);

		private final Class<?> baseClass;

		Hull(
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
			final Hull[] params ) {
		if (contains(
				params,
				Hull.INDEX_ID)) {
			options.add(PropertyManagement.newOption(
					Hull.INDEX_ID,
					"hid",
					"Index Identifier for Centroids",
					true));
		}
		if (contains(
				params,
				Hull.ZOOM_LEVEL)) {
			options.add(PropertyManagement.newOption(
					Hull.ZOOM_LEVEL,
					"hzl",
					"Zoom Level Number",
					true));
		}
		if (contains(
				params,
				Hull.DATA_NAMESPACE_URI)) {
			options.add(PropertyManagement.newOption(
					Hull.DATA_NAMESPACE_URI,
					"hns",
					"Data Type Namespace for a centroid item",
					true));
		}
		if (contains(
				params,
				Hull.REDUCER_COUNT)) {
			options.add(PropertyManagement.newOption(
					Hull.REDUCER_COUNT,
					"hrc",
					"Centroid Reducer Count",
					true));
		}
		if (contains(
				params,
				Hull.EXTRACTOR_CLASS)) {
			options.add(PropertyManagement.newOption(
					Hull.EXTRACTOR_CLASS,
					"hce",
					"Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor",
					true));
		}
		if (contains(
				params,
				Hull.PROJECTION_CLASS)) {
			options.add(PropertyManagement.newOption(
					Hull.PROJECTION_CLASS,
					"hpe",
					"Class to project on to 2D space. Implements mil.nga.giat.geowave.analytics.tools.Projection",
					true));
		}
		if (contains(
				params,
				Hull.DATA_TYPE_ID)) {
			options.add(PropertyManagement.newOption(
					Hull.DATA_TYPE_ID,
					"hdt",
					"Data Type ID for a centroid item",
					true));
		}
		if (contains(
				params,
				Hull.WRAPPER_FACTORY_CLASS)) {
			options.add(PropertyManagement.newOption(
					Hull.WRAPPER_FACTORY_CLASS,
					"hfc",
					"Class to create analytic item to capture hulls. Implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory",
					true));
		}

	}

	private static boolean contains(
			final Hull[] params,
			final Hull option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
