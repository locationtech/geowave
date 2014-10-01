package mil.nga.giat.geowave.index;

import mil.nga.giat.geowave.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.index.sfc.tiered.TieredSFCIndexFactory;

public interface NumericIndexStrategyFactory
{
	public NumericIndexStrategy createIndexStrategy(
			DataType dataType );

	public static enum DataType {
		RASTER,
		VECTOR,
		OTHER
	}

	public static class SpatialFactory implements
			NumericIndexStrategyFactory
	{
		public static final int LONGITUDE_BITS = 31;
		public static final int LATITUDE_BITS = 31;
		private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
			new LongitudeDefinition(),
			new LatitudeDefinition()
		};

		@Override
		public NumericIndexStrategy createIndexStrategy(
				final DataType dataType ) {
			switch (dataType) {
				case OTHER:
					throw new UnsupportedOperationException(
							"There is not a default spatial index strategy for 'OTHER' data types");
				case RASTER:
					return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
							SPATIAL_DIMENSIONS,
							new int[] {
								LONGITUDE_BITS,
								LONGITUDE_BITS - 1
							// ensure that the latitude is 1 bit less than
							// longitude to
							// produce square tiles in EPSG:4326
							},
							SFCType.HILBERT);
				default:
				case VECTOR:
					return TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
							SPATIAL_DIMENSIONS,
							new int[] {
								LONGITUDE_BITS,
								LATITUDE_BITS
							},
							SFCType.HILBERT);
			}
		}
	}

	public static class SpatialTemporalFactory implements
			NumericIndexStrategyFactory
	{
		private static final int LONGITUDE_BITS = 20;
		private static final int LATITUDE_BITS = 20;
		private static final int TIME_BITS = 20;
		private static final NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS = new NumericDimensionDefinition[] {
			new LongitudeDefinition(),
			new LatitudeDefinition(),
			new TimeDefinition(
					Unit.YEAR),
		};

		@Override
		public NumericIndexStrategy createIndexStrategy(
				final DataType dataType ) {
			switch (dataType) {
				case OTHER:
					throw new UnsupportedOperationException(
							"There is not a default spatial-temporal index strategy for 'OTHER' data types");
				case RASTER:
					return TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
							SPATIAL_TEMPORAL_DIMENSIONS,
							new int[] {
								LONGITUDE_BITS,
								LONGITUDE_BITS - 1,
								// ensure that the latitude is 1 bit less than
								// longitude
								// to produce square tiles in EPSG:4326
								TIME_BITS
							},
							SFCType.HILBERT);
				case VECTOR:
				default:
					return TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
							SPATIAL_TEMPORAL_DIMENSIONS,
							new int[] {
								LONGITUDE_BITS,
								LATITUDE_BITS,
								TIME_BITS
							},
							SFCType.HILBERT);
			}
		}
	}
}
