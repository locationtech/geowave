package org.locationtech.geowave.core.store.query.options;

public interface CommonQueryOptions
{
	public double[] getMaxResolutionSubsamplingPerDimension();

	/**
	 *
	 * @return the max range decomposition to use when computing query ranges
	 */
	public Integer getMaxRangeDecomposition();

	/**
	 *
	 * @return Limit the number of data items to return
	 */
	public Integer getLimit();

	/**
	 *
	 * @return authorizations to apply to the query in addition to the
	 *         authorizations assigned to the data store as a whole.
	 */
	public String[] getAuthorizations();

}