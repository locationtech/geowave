package mil.nga.giat.geowave.core.index.sfc.data;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;

/**
 * The Binned Numeric Dataset class creates an object that associates a
 * multi-dimensional index range to a particular bin ID.
 * 
 */
public class BinnedNumericDataset implements
		MultiDimensionalNumericData
{
	private final byte[] binId;
	private final MultiDimensionalNumericData indexRanges;

	/**
	 * 
	 * @param binId
	 *            a unique ID associated with the BinnedQuery object
	 * @param indexRanges
	 *            multi-dimensional range data
	 */
	public BinnedNumericDataset(
			final byte[] binId,
			final MultiDimensionalNumericData indexRanges ) {
		this.binId = binId;
		this.indexRanges = indexRanges;
	}

	/**
	 * @return an array of NumericData objects associated with this object.
	 */
	@Override
	public NumericData[] getDataPerDimension() {
		return indexRanges.getDataPerDimension();
	}

	/**
	 * @return an array of max values associated with each dimension
	 */
	@Override
	public double[] getMaxValuesPerDimension() {
		return indexRanges.getMaxValuesPerDimension();
	}

	/**
	 * @return an array of min values associated with each dimension
	 */
	@Override
	public double[] getMinValuesPerDimension() {
		return indexRanges.getMinValuesPerDimension();
	}

	/**
	 * @return an array of centroid values associated with each dimension
	 */
	@Override
	public double[] getCentroidPerDimension() {
		return indexRanges.getCentroidPerDimension();
	}

	/**
	 * @return the number of total dimensions
	 */
	@Override
	public int getDimensionCount() {
		return indexRanges.getDimensionCount();
	}

	/**
	 * @return a unique ID associated with this object
	 */
	public byte[] getBinId() {
		return binId;
	}

	/**
	 * This method is responsible for translating a query into appropriate
	 * normalized and binned (if necessary) queries that can be used by the
	 * underlying index implementation. For example, for unbounded dimensions
	 * such as time, an incoming query of July 2012 to July 2013 may get
	 * translated into 2 binned queries representing the 2012 portion of the
	 * query and the 2013 portion, each normalized to millis from the beginning
	 * of the year.
	 * 
	 * @param numericData
	 *            the incoming query into the index implementation, to be
	 *            translated into normalized, binned queries
	 * @param dimensionDefinitions
	 *            the definition for the dimensions
	 * @return normalized indexes
	 */
	public static BinnedNumericDataset[] applyBins(
			MultiDimensionalNumericData numericData,
			NumericDimensionDefinition[] dimensionDefinitions ) {
		if (dimensionDefinitions.length == 0) {
			return new BinnedNumericDataset[0];
		}
		BinRange[][] binRangesPerDimension = new BinRange[dimensionDefinitions.length][];
		int numBinnedQueries = 1;
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			binRangesPerDimension[d] = dimensionDefinitions[d].getNormalizedRanges(numericData.getDataPerDimension()[d]);
			numBinnedQueries *= binRangesPerDimension[d].length;
		}
		// now we need to combine all permutations of bin ranges into
		// BinnedQuery objects
		BinnedNumericDataset[] binnedQueries = new BinnedNumericDataset[numBinnedQueries];
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			for (int b = 0; b < binRangesPerDimension[d].length; b++) {
				for (int i = b; i < numBinnedQueries; i += binRangesPerDimension[d].length) {
					final NumericData[] rangePerDimension;
					if (binnedQueries[i] == null) {
						rangePerDimension = new NumericRange[dimensionDefinitions.length];
						binnedQueries[i] = new BinnedNumericDataset(
								binRangesPerDimension[d][b].getBinId(),
								new BasicNumericDataset(
										rangePerDimension));
					}
					else {
						// because binned queries were intended to be immutable,
						// re-instantiate the object
						rangePerDimension = binnedQueries[i].getDataPerDimension();

						byte[] combinedBinId = ByteArrayUtils.combineArrays(
								binnedQueries[i].getBinId(),
								binRangesPerDimension[d][b].getBinId());
						binnedQueries[i] = new BinnedNumericDataset(
								combinedBinId,
								new BasicNumericDataset(
										rangePerDimension));
					}

					rangePerDimension[d] = new NumericRange(
							binRangesPerDimension[d][b].getNormalizedMin(),
							binRangesPerDimension[d][b].getNormalizedMax());
				}
			}
		}
		return binnedQueries;
	}

	@Override
	public boolean isEmpty() {
		return indexRanges.isEmpty();
	}
}
