package mil.nga.giat.geowave.core.index.sfc.data;

/**
 * Interface used to define numeric data associated with a space filling curve.
 * 
 */
public interface NumericData extends
		java.io.Serializable
{
	public double getMin();

	public double getMax();

	public double getCentroid();

	public boolean isRange();
}
