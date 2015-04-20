package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.db.AdapterStoreFactory;
import mil.nga.giat.geowave.analytic.db.BasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.analytic.db.IndexStoreFactory;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.model.IndexModelBuilder;

import org.apache.commons.cli.Option;

public class CommonParameters
{
	public enum Common
			implements
			ParameterEnum {
		DIMENSION_EXTRACT_CLASS(
				DimensionExtractor.class),
		ACCUMULO_CONNECT_FACTORY(
				BasicAccumuloOperationsFactory.class),
		ADAPTER_STORE_FACTORY(
				AdapterStoreFactory.class),
		INDEX_STORE_FACTORY(
				IndexStoreFactory.class),
		DISTANCE_FUNCTION_CLASS(
				DistanceFn.class),
		INDEX_MODEL_BUILDER_CLASS(
				IndexModelBuilder.class);

		private final Class<?> baseClass;

		Common(
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
			Set<Option> options,
			Common[] params ) {
		if (contains(
				params,
				Common.DIMENSION_EXTRACT_CLASS)) {
			options.add(PropertyManagement.newOption(
					Common.DIMENSION_EXTRACT_CLASS,
					"dde",
					"Dimension Extractor Class implements mil.nga.giat.geowave.analytics.extract.DimensionExtractor",
					true));
		}
		if (contains(
				params,
				Common.ADAPTER_STORE_FACTORY)) {
			options.add(PropertyManagement.newOption(
					Common.ADAPTER_STORE_FACTORY,
					"caf",
					"Adapter Store factory implements mil.nga.giat.geowave.analytics.tools.dbops.AdapterStoreFactory",
					true));
		}
		if (contains(
				params,
				Common.INDEX_STORE_FACTORY)) {
			options.add(PropertyManagement.newOption(
					Common.INDEX_STORE_FACTORY,
					"cif",
					"Index Store factory implements mil.nga.giat.geowave.analytics.tools.dbops.IndexStoreFactory",
					true));
		}
		if (contains(
				params,
				Common.DISTANCE_FUNCTION_CLASS)) {
			options.add(PropertyManagement.newOption(
					Common.DISTANCE_FUNCTION_CLASS,
					"cdf",
					"Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn",
					true));
		}
		if (contains(
				params,
				Common.INDEX_MODEL_BUILDER_CLASS)) {
			options.add(PropertyManagement.newOption(
					Common.INDEX_MODEL_BUILDER_CLASS,
					"cim",
					"Class implements mil.nga.giat.geowave.analytics.tools.model.IndexModelBuilder",
					true));
		}

	}

	private static boolean contains(
			Common[] params,
			Common option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
