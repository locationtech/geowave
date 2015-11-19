package mil.nga.giat.geowave.core.index.sfc.data;

import java.util.Arrays;

/**
 * The Basic Index Result class creates an object associated with a generic
 * query. This class can be used when the dimensions and/or axis are generic.
 * 
 */
public class BasicNumericDataset implements
		MultiDimensionalNumericData
{

	private final NumericData[] dataPerDimension;

	/**
	 * Open ended/unconstrained
	 */
	public BasicNumericDataset() {
		this.dataPerDimension = new NumericData[0];
	}

	/**
	 * Constructor used to create a new Basic Numeric Dataset object.
	 * 
	 * @param dataPerDimension
	 *            an array of numeric data objects
	 */
	public BasicNumericDataset(
			final NumericData[] dataPerDimension ) {
		this.dataPerDimension = dataPerDimension;
	}

	/**
	 * @return all of the maximum values (for each dimension)
	 */
	@Override
	public double[] getMaxValuesPerDimension() {
		NumericData[] ranges = getDataPerDimension();
		double[] maxPerDimension = new double[ranges.length];
		for (int d = 0; d < ranges.length; d++) {
			maxPerDimension[d] = ranges[d].getMax();
		}
		return maxPerDimension;
	}

	/**
	 * @return all of the minimum values (for each dimension)
	 */
	@Override
	public double[] getMinValuesPerDimension() {
		NumericData[] ranges = getDataPerDimension();
		double[] minPerDimension = new double[ranges.length];
		for (int d = 0; d < ranges.length; d++) {
			minPerDimension[d] = ranges[d].getMin();
		}
		return minPerDimension;
	}

	/**
	 * @return all of the centroid values (for each dimension)
	 */
	@Override
	public double[] getCentroidPerDimension() {
		NumericData[] ranges = getDataPerDimension();
		double[] centroid = new double[ranges.length];
		for (int d = 0; d < ranges.length; d++) {
			centroid[d] = ranges[d].getCentroid();
		}
		return centroid;
	}

	/**
	 * 
	 * @return an array of NumericData objects
	 */
	@Override
	public NumericData[] getDataPerDimension() {
		return dataPerDimension;
	}

	/**
	 * @return the number of dimensions associated with this data set
	 */
	@Override
	public int getDimensionCount() {
		return dataPerDimension.length;
	}

	@Override
	public boolean isEmpty() {
		return this.dataPerDimension == null || this.dataPerDimension.length == 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(dataPerDimension);
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		BasicNumericDataset other = (BasicNumericDataset) obj;
		if (!Arrays.equals(
				dataPerDimension,
				other.dataPerDimension)) return false;
		return true;
	}

}
