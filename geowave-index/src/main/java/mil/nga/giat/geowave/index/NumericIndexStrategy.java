package mil.nga.giat.geowave.index;

import java.util.List;

import mil.nga.giat.geowave.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.tiered.TieredSFCIndexFactory;

/**
 * Interface which defines a numeric index strategy.
 * 
 */
public interface NumericIndexStrategy extends
		Persistable
{
	public static class SpatialFactory
	{
		public static final int LONGITUDE_BITS = 31;
		public static final int LATITUDE_BITS = 31;
		private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
			new LongitudeDefinition(),
			new LatitudeDefinition()
		};

		public static NumericIndexStrategy createIndexStrategy() {
			return TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
					SPATIAL_DIMENSIONS,
					new int[] {
						LONGITUDE_BITS,
						LATITUDE_BITS
					},
					SFCType.HILBERT);
		}
	}

	public static class SpatialTemporalFactory
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

		public static NumericIndexStrategy createIndexStrategy() {
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

	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @return a List of query ranges
	 */
	public List<ByteArrayRange> getQueryRanges(
			MultiDimensionalNumericData indexedRange );

	/**
	 * Returns a list of query ranges for an specified numeric range.
	 * 
	 * @param indexedRange
	 *            defines the numeric range for the query
	 * @param maxRangeDecomposition
	 *            the maximum number of ranges provided by a single query
	 *            decomposition
	 * @return a List of query ranges
	 */
	public List<ByteArrayRange> getQueryRanges(
			MultiDimensionalNumericData indexedRange,
			int maxRangeDecomposition );

	/**
	 * Returns a list of id's for insertion.
	 * 
	 * @param indexedData
	 *            defines the numeric data to be indexed
	 * @return a List of insertion ID's
	 */
	public List<ByteArrayId> getInsertionIds(
			MultiDimensionalNumericData indexedData );

	/**
	 * Returns an array of dimension definitions that defines this index
	 * strategy, the array is in the order that is expected within
	 * multidimensional numeric data that is passed to this index strategy
	 * 
	 * @return the ordered array of dimension definitions that represents this
	 *         index strategy
	 */
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions();

	/**
	 * 
	 * @return a unique ID associated with the index strategy
	 */
	public String getId();
}
