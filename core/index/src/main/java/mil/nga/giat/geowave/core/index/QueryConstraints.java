package mil.nga.giat.geowave.core.index;

public interface QueryConstraints
{
	public int getDimensionCount();

	/**
	 * Unconstrained?
	 * 
	 * @return return if unconstrained on a dimension
	 */
	public boolean isEmpty();
}
