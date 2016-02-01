package mil.nga.giat.geowave.core.index;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

public class IndexUtils
{
	public static MultiDimensionalNumericData getFullBounds(
			final NumericIndexStrategy indexStrategy ) {
		return getFullBounds(indexStrategy.getOrderedDimensionDefinitions());
	}

	public static MultiDimensionalNumericData getFullBounds(
			final NumericDimensionDefinition[] dimensionDefinitions ) {
		final NumericRange[] boundsPerDimension = new NumericRange[dimensionDefinitions.length];
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			boundsPerDimension[d] = dimensionDefinitions[d].getBounds();
		}
		return new BasicNumericDataset(
				boundsPerDimension);
	}

	public static final double getDimensionalBitsUsed(
			final NumericIndexStrategy indexStrategy,
			final double[] dataRangePerDimension ) {
		double result = Long.MAX_VALUE;
		if (dataRangePerDimension.length == 0) {
			return 0;
		}
		final double cellRangePerDimension[] = indexStrategy.getHighestPrecisionIdRangePerDimension();
		final double bitsPerDimension[] = getBitsPerDimension(
				indexStrategy,
				cellRangePerDimension);
		for (int d = 0; d < dataRangePerDimension.length; d++) {
			final double bitsUsed = bitsPerDimension[d] - log2(Math.ceil(dataRangePerDimension[d] / cellRangePerDimension[d]));
			result = Math.min(
					bitsUsed,
					result);
		}
		// The least constraining dimension uses the least amount of bits of
		// fixed bits from the left.
		// For example, half of the world latitude is 1 bit, 1/4 of the world is
		// 2 bits etc.
		// Use the least constraining dimension, but multiply by the
		// # of dimensions.
		return result * cellRangePerDimension.length;
	}

	private static final double[] getBitsPerDimension(
			final NumericIndexStrategy indexStrategy,
			final double[] rangePerDimension ) {
		final NumericDimensionDefinition dim[] = indexStrategy.getOrderedDimensionDefinitions();
		final double result[] = new double[rangePerDimension.length];
		for (int d = 0; d < rangePerDimension.length; d++) {
			result[d] += Math.ceil(log2((dim[d].getRange() / rangePerDimension[d])));
		}
		return result;
	}

	private static double log2(
			final double v ) {
		return Math.log(v) / Math.log(2);
	}
}
