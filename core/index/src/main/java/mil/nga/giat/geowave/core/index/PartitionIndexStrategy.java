package mil.nga.giat.geowave.core.index;

import java.util.Set;

public interface PartitionIndexStrategy<QueryRangeType extends QueryConstraints, EntryRangeType> extends
		IndexStrategy
{
	public Set<ByteArrayId> getInsertionPartitionKeys(
			EntryRangeType insertionData );

	public Set<ByteArrayId> getQueryPartitionKeys(
			QueryRangeType queryData,
			IndexMetaData... hints );

	/***
	 * Get the offset in bytes before the dimensional index. This can accounts
	 * for tier IDs and bin IDs
	 * 
	 * @return the byte offset prior to the dimensional index
	 */
	public int getPartitionKeyLength();

}
