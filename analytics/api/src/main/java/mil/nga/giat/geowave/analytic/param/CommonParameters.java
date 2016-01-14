package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.model.IndexModelBuilder;

public class CommonParameters
{
	public enum Common
			implements
			ParameterEnum {
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class,
				"dde",
				"Dimension Extractor Class implements mil.nga.giat.geowave.analytics.extract.DimensionExtractor",
				true),
		DISTANCE_FUNCTION_CLASS(
				DistanceFn.class,
				"cdf",
				"Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn",
				true),
		INDEX_MODEL_BUILDER_CLASS(
				IndexModelBuilder.class,
				"cim",
				"Class implements mil.nga.giat.geowave.analytics.tools.model.IndexModelBuilder",
				true);

		private final ParameterHelper<?> helper;

		Common(
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
