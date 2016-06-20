package mil.nga.giat.geowave.core.index;

import java.util.List;

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
			List<ByteArrayId> insertionIds );

	/**
	 * Update the aggregation result by removing the entries provided
	 *
	 * @param insertionIds
	 *            the new indices to compute an updated aggregation result on
	 */
	public void insertionIdsRemoved(
			List<ByteArrayId> insertionIds );
}
