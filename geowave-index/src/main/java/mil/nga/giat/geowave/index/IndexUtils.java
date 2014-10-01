package mil.nga.giat.geowave.index;

import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;

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
}
