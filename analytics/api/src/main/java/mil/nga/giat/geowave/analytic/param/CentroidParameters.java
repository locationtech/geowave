package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;

import org.apache.commons.cli.Option;

public class CentroidParameters
{
	public enum Centroid
			implements
			ParameterEnum {
		INDEX_ID(
				String.class),
		DATA_TYPE_ID(
				String.class),
		DATA_NAMESPACE_URI(
				String.class),
		CONVERGANCE_TOLERANCE(
				Double.class),
		DISTORTION_TABLE_NAME(
				String.class),
		EXTRACTOR_CLASS(
				CentroidExtractor.class),
		WRAPPER_FACTORY_CLASS(
				AnalyticItemWrapperFactory.class),
		ZOOM_LEVEL(
				Integer.class);

		private final Class<?> baseClass;

		Centroid(
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
			final Centroid[] params ) {
		if (contains(
				params,
				Centroid.INDEX_ID)) {
			options.add(PropertyManagement.newOption(
					Centroid.INDEX_ID,
					"cid",
					"Index Identifier for Centroids",
					true));
		}
		if (contains(
				params,
				Centroid.DISTORTION_TABLE_NAME)) {
			options.add(PropertyManagement.newOption(
					Centroid.DISTORTION_TABLE_NAME,
					"cdt",
					"The name of the Accumulo holding the information distortion for each batch of K-Means",
					true));
		}
		if (contains(
				params,
				Centroid.ZOOM_LEVEL)) {
			options.add(PropertyManagement.newOption(
					Centroid.ZOOM_LEVEL,
					"czl",
					"Zoom Level Number",
					true));
		}

		if (contains(
				params,
				Centroid.EXTRACTOR_CLASS)) {
			options.add(PropertyManagement.newOption(
					Centroid.EXTRACTOR_CLASS,
					"cce",
					"Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor",
					true));
		}
		if (contains(
				params,
				Centroid.WRAPPER_FACTORY_CLASS)) {
			options.add(PropertyManagement.newOption(
					Centroid.WRAPPER_FACTORY_CLASS,
					"cfc",
					"A factory class that implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory",
					true));
		}

		if (contains(
				params,
				Centroid.DATA_TYPE_ID)) {
			options.add(PropertyManagement.newOption(
					Centroid.DATA_TYPE_ID,
					"cdt",
					"Data Type ID for a centroid item",
					true));
		}

		if (contains(
				params,
				Centroid.DATA_NAMESPACE_URI)) {
			options.add(PropertyManagement.newOption(
					Centroid.DATA_NAMESPACE_URI,
					"cns",
					"Data Type Namespace for centroid item",
					true));
		}
	}

	private static boolean contains(
			final Centroid[] params,
			final Centroid option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
