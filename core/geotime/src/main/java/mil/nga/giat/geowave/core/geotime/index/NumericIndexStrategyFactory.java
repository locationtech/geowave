package mil.nga.giat.geowave.core.geotime.index;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

public interface NumericIndexStrategyFactory
{
	public NumericIndexStrategy createIndexStrategy(
			DataType dataType );

	public NumericDimensionDefinition[] getFactoryDefinition();

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
		protected static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
			new LongitudeDefinition(),
			new LatitudeDefinition(
					true)
		// just use the same range for latitude to make square sfc values in
		// decimal degrees (EPSG:4326)
		};
		public static final int[] DEFINED_BITS_OF_PRECISION = new int[] {
			0,
			1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9,
			10,
			11,
			13,
			18,
			31
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
								LATITUDE_BITS
							},
							SFCType.HILBERT);
				default:
				case VECTOR:
					return TieredSFCIndexFactory.createDefinedPrecisionTieredStrategy(
							SPATIAL_DIMENSIONS,
							new int[][] {
								DEFINED_BITS_OF_PRECISION.clone(),
								DEFINED_BITS_OF_PRECISION.clone()
							},
							SFCType.HILBERT);
			}
		}

		@Override
		public NumericDimensionDefinition[] getFactoryDefinition() {
			return SPATIAL_DIMENSIONS;
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
			new LatitudeDefinition(
					true),
			// just use the same range for latitude to make square sfc values in
			// decimal degrees (EPSG:4326)
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
								LATITUDE_BITS,
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

		@Override
		public NumericDimensionDefinition[] getFactoryDefinition() {
			return SPATIAL_TEMPORAL_DIMENSIONS;
		}
	}
}
