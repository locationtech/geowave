package mil.nga.giat.geowave.core.geotime.ingest;

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
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.ingest.index.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class SpatialTemporalDimensionalityTypeProvider implements
		IngestDimensionalityTypeProviderSpi
{
	private final SpatialTemporalOptions options = new SpatialTemporalOptions();
	private static final ByteArrayId DEFAULT_SPATIAL_TEMPORAL_ID = new ByteArrayId(
			"SPATIAL_TEMPORAL_IDX");

	public SpatialTemporalDimensionalityTypeProvider() {}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial-temporal";
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
	public Object getOptions() {
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
		if (options.pointOnly) {
			return new CustomIdIndex(
					TieredSFCIndexFactory.createDefinedPrecisionTieredStrategy(
							dimensions,
							new int[][] {
								new int[] {
									0,
									0,
									0
								},
								new int[] {
									options.bias.getSpatialPrecision(),
									options.bias.getSpatialPrecision(),
									options.bias.getTemporalPrecision()
								}
							},
							SFCType.HILBERT),
					new BasicIndexModel(
							fields),
					DEFAULT_SPATIAL_TEMPORAL_ID);
		}
		else {
			return new CustomIdIndex(
					TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
							dimensions,
							new int[] {
								options.bias.getSpatialPrecision(),
								options.bias.getSpatialPrecision(),
								options.bias.getTemporalPrecision()
							},
							SFCType.HILBERT),
					new BasicIndexModel(
							fields),
					DEFAULT_SPATIAL_TEMPORAL_ID);
		}
	}

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	private static class SpatialTemporalOptions
	{
		@Parameter(names = {
			"period"
		}, required = false, description = "The periodicity of the temporal dimension.  Because time is continuous, it is binned at this interval.", converter = UnitConverter.class)
		protected Unit periodicity = Unit.YEAR;

		@Parameter(names = {
			"bias"
		}, required = false, description = "The bias of the spatial-temporal index. There can be more precision given to time or space if necessary.", converter = BiasConverter.class)
		protected Bias bias = Bias.BALANCED;
		@Parameter(names = {
			"pointTimestampOnly"
		}, required = false, description = "The index will only be good at handling points and timestamps and will not be optimized for handling lines/polys or time ranges.  The default behavior is to handle any geometry and time ranges well.")
		protected boolean pointOnly = false;
	}

	protected static enum Bias {
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
						"Value " + value + "can not be converted to an index bias. " + "Available values are: " + StringUtils.join(
								Bias.values(),
								", ").toLowerCase());
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
						"Value " + value + "can not be converted to Unit. " + "Available values are: " + StringUtils.join(
								Unit.values(),
								", ").toLowerCase());
			}
			return convertedValue;
		}
	}

	public static class SpatialTemporalIndexBuilder
	{
		private final SpatialTemporalOptions options;

		public SpatialTemporalIndexBuilder() {
			options = new SpatialTemporalOptions();
		}

		private SpatialTemporalIndexBuilder(
				final SpatialTemporalOptions options ) {
			this.options = options;
		}

		public SpatialTemporalIndexBuilder setPointOnly(
				final boolean pointOnly ) {
			options.pointOnly = pointOnly;
			return new SpatialTemporalIndexBuilder(
					options);
		}

		public SpatialTemporalIndexBuilder setBias(
				final Bias bias ) {
			options.bias = bias;
			return new SpatialTemporalIndexBuilder(
					options);
		}

		public SpatialTemporalIndexBuilder setPeriodicity(
				final Unit periodicity ) {
			options.periodicity = periodicity;
			return new SpatialTemporalIndexBuilder(
					options);
		}

		public PrimaryIndex createIndex() {
			return internalCreatePrimaryIndex(options);
		}
	}
}
