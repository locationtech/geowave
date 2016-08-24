package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

/**
 * This is responsible for persisting secondary index entries
 */
public interface SecondaryIndexDataStore
{
	/**
	 * Stores a secondary index entry that will require a join against the
	 * primary index upon lookup.
	 * 
	 * @param secondaryIndexId
	 * @param indexedAttributeValue
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @param primaryIndexId
	 * @param primaryIndexRowId
	 * @param attributeVisibility
	 */
	public void storeJoinEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId primaryIndexId,
			ByteArrayId primaryIndexRowId,
			ByteArrayId attributeVisibility );

	/**
	 * Stores a secondary index entry that will not require a join against the
	 * primary index upon lookup.
	 * 
	 * @param secondaryIndexId
	 * @param indexedAttributeValue
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @param dataId
	 * @param attributeVisibility
	 * @param attributes
	 */
	public void storeEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId dataId,
			ByteArrayId attributeVisibility,
			List<FieldInfo<?>> attributes );

	/**
	 * Performs a scan of the appropriate secondary index table
	 * 
	 * @param secondaryIndexId
	 * @param scanRanges
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @param visibility
	 * @return an iterator over the results
	 */
	public CloseableIterator<Object> scan(
			ByteArrayId secondaryIndexId,
			List<ByteArrayRange> scanRanges,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			String... visibility );

	/**
	 * 
	 * @param secondaryIndex
	 * @param primaryIndexId
	 * @param indexedAttributes
	 */
	public void delete(
			final SecondaryIndex<?> secondaryIndex,
			final List<FieldInfo<?>> indexedAttributes );

	public void flush();

	public void removeAll();
}