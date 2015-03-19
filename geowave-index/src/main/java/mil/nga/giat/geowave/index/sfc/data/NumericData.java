package mil.nga.giat.geowave.index.sfc.data;

/**
 * Interface used to define numeric data associated with a space filling curve.
 * 
 */
public interface NumericData
{
	public double getMin();

	public double getMax();

	public double getCentroid();

	public boolean isRange();
}
