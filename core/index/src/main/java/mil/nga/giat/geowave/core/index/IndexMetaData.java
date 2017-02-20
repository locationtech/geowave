package mil.nga.giat.geowave.core.index;

public interface IndexMetaData extends
		Persistable,
		Mergeable
{
	/**
	 * Update the aggregation result using the new entry provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsAdded(
			InsertionIds insertionIds );

	/**
	 * Update the aggregation result by removing the entries provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsRemoved(
			InsertionIds insertionIds );
}
