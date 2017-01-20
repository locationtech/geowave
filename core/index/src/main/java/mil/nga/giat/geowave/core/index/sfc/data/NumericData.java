package mil.nga.giat.geowave.core.index.sfc.data;

import mil.nga.giat.geowave.core.index.Persistable;

/**
 * Interface used to define numeric data associated with a space filling curve.
 * 
 */
public interface NumericData extends
		java.io.Serializable,
		Persistable
{
	public double getMin();

	public double getMax();

	public double getCentroid();

	public boolean isRange();
}
