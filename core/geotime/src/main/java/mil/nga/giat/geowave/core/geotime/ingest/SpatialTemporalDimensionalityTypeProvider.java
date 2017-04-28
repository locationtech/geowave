package mil.nga.giat.geowave.core.geotime.ingest;

import java.util.Locale;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.xz.XZHierarchicalIndexFactory;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions.BaseIndexBuilder;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;

public class SpatialTemporalDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi
{
	private final SpatialTemporalOptions options = new SpatialTemporalOptions();
	private static final String DEFAULT_SPATIAL_TEMPORAL_ID_STR = "SPATIAL_TEMPORAL_IDX";

	public SpatialTemporalDimensionalityTypeProvider() {}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial_temporal";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Geometry and Time.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just lower than spatial so that the default
		// will be spatial over spatial-temporal
		return 5;
	}

	@Override
	public DimensionalityTypeOptions getOptions() {
		return options;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		return internalCreatePrimaryIndex(options);
	}

	private static PrimaryIndex internalCreatePrimaryIndex(
			final SpatialTemporalOptions options ) {
		// TODO should we use different default IDs for all the different
		// options, for now lets just use one
		final NumericDimensionField[] fields = new NumericDimensionField[] {
			new LongitudeField(),
			new LatitudeField(
					true),
			new TimeField(
					options.periodicity)
		};
		final NumericDimensionDefinition[] dimensions = new NumericDimensionDefinition[] {
			new LongitudeDefinition(),
			new LatitudeDefinition(
					true),
			new TimeDefinition(
					options.periodicity)
		};
		final String combinedId = DEFAULT_SPATIAL_TEMPORAL_ID_STR + "_" + options.bias + "_" + options.periodicity;
		return new CustomIdIndex(
				XZHierarchicalIndexFactory.createFullIncrementalTieredStrategy(
						dimensions,
						new int[] {
							options.bias.getSpatialPrecision(),
							options.bias.getSpatialPrecision(),
							options.bias.getTemporalPrecision()
						},
						SFCType.HILBERT,
						options.maxDuplicates),
				new BasicIndexModel(
						fields),
				new ByteArrayId(
						combinedId));
	}

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	private static class SpatialTemporalOptions implements
			DimensionalityTypeOptions
	{
		@Parameter(names = {
			"--period"
		}, required = false, description = "The periodicity of the temporal dimension.  Because time is continuous, it is binned at this interval.", converter = UnitConverter.class)
		protected Unit periodicity = Unit.YEAR;

		@Parameter(names = {
			"--bias"
		}, required = false, description = "The bias of the spatial-temporal index. There can be more precision given to time or space if necessary.", converter = BiasConverter.class)
		protected Bias bias = Bias.BALANCED;
		@Parameter(names = {
			"--maxDuplicates"
		}, required = false, description = "The max number of duplicates per dimension range.  The default is 2 per range (for example lines and polygon timestamp data would be up to 4 because its 2 dimensions, and line/poly time range data would be 8).")
		protected long maxDuplicates = -1;
	}

	public static enum Bias {
		TEMPORAL,
		BALANCED,
		SPATIAL;
		// converter that will be used later
		public static Bias fromString(
				final String code ) {

			for (final Bias output : Bias.values()) {
				if (output.toString().equalsIgnoreCase(
						code)) {
					return output;
				}
			}

			return null;
		}

		protected int getSpatialPrecision() {
			switch (this) {
				case SPATIAL:
					return 25;
				case TEMPORAL:
					return 10;
				case BALANCED:
				default:
					return 20;
			}
		}

		protected int getTemporalPrecision() {
			switch (this) {
				case SPATIAL:
					return 10;
				case TEMPORAL:
					return 40;
				case BALANCED:
				default:
					return 20;
			}
		}
	}

	public static class BiasConverter implements
			IStringConverter<Bias>
	{
		@Override
		public Bias convert(
				final String value ) {
			final Bias convertedValue = Bias.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value " + value + "can not be converted to an index bias. " + "Available values are: "
								+ StringUtils.join(
										Bias.values(),
										", ").toLowerCase(
										Locale.ENGLISH));
			}
			return convertedValue;
		}

	}

	public static class UnitConverter implements
			IStringConverter<Unit>
	{

		@Override
		public Unit convert(
				final String value ) {
			final Unit convertedValue = Unit.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value " + value + "can not be converted to Unit. " + "Available values are: "
								+ StringUtils.join(
										Unit.values(),
										", ").toLowerCase(
										Locale.ENGLISH));
			}
			return convertedValue;
		}
	}

	public static class SpatialTemporalIndexBuilder extends
			BaseIndexBuilder<SpatialTemporalIndexBuilder>
	{
		private final SpatialTemporalOptions options;

		public SpatialTemporalIndexBuilder() {
			options = new SpatialTemporalOptions();
		}

		public SpatialTemporalIndexBuilder setBias(
				final Bias bias ) {
			options.bias = bias;
			return this;
		}

		public SpatialTemporalIndexBuilder setPeriodicity(
				final Unit periodicity ) {
			options.periodicity = periodicity;
			return this;
		}

		public SpatialTemporalIndexBuilder setMaxDuplicates(
				final long maxDuplicates ) {
			options.maxDuplicates = maxDuplicates;
			return this;
		}

		@Override
		public PrimaryIndex createIndex() {
			return createIndex(internalCreatePrimaryIndex(options));
		}
	}

	public static boolean isSpatialTemporal(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null)
				|| (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length < 3) {
			return false;
		}
		boolean hasLat = false, hasLon = false, hasTime = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof TimeDefinition) {
				hasTime = true;
			}
			else if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasTime && hasLat && hasLon;
	}
}
