package mil.nga.giat.geowave.core.store.data;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This class models all of the necessary information for persisting data in
 * Accumulo (following the common index model) and is used internally within
 * GeoWave as an intermediary object between the direct storage format and the
 * native data format. It also contains information about the persisted object
 * within a particular index such as the insertion ID in the index and the
 * number of duplicates for this entry in the index, and is used when reading
 * data from the index.
 */
public class IndexedPersistenceEncoding extends
		PersistenceEncoding
{
	private final ByteArrayId indexId;
	private final int duplicateCount;

	public IndexedPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexId,
			final int duplicateCount,
			final PersistentDataset<? extends CommonIndexValue> commonData ) {
		super(
				adapterId,
				dataId,
				commonData);
		this.indexId = indexId;
		this.duplicateCount = duplicateCount;
	}

	/**
	 * Return the index ID, this is the ID that the entry inserted at given by
	 * the index
	 * 
	 * @return the index ID
	 */
	public ByteArrayId getIndexInsertionId() {
		return indexId;
	}

	@Override
	public boolean isDeduplicationEnabled() {
		return duplicateCount >= 0;
	}

	/**
	 * Return the number of duplicates for this entry. Entries are duplicated
	 * when a single row ID is insufficient to index it.
	 * 
	 * @return the number of duplicates
	 */
	public int getDuplicateCount() {
		return duplicateCount;
	}

	/**
	 * Return a flag indicating if the entry has any duplicates
	 * 
	 * @return is it duplicated?
	 */
	public boolean isDuplicated() {
		return duplicateCount > 0;
	}

}
