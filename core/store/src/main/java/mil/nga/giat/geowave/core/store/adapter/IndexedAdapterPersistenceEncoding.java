package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is an implements of persistence encoding that also contains all of the
 * extended data values used to form the native type supported by this adapter.
 * It also contains information about the persisted object within a particular
 * index such as the insertion ID in the index and the number of duplicates for
 * this entry in the index, and is used when reading data from the index.
 */
public class IndexedAdapterPersistenceEncoding extends
		CommonIndexedPersistenceEncoding
{
	private final PersistentDataset<Object> adapterExtendedData;

	public IndexedAdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexInsertionId,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<byte[]> unknownData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				indexInsertionId,
				duplicateCount,
				commonData,
				unknownData);
		this.adapterExtendedData = adapterExtendedData;
	}

	/**
	 * This returns a representation of the custom fields for the data adapter
	 * 
	 * @return the extended data beyond the common index fields that are
	 *         provided by the adapter
	 */
	public PersistentDataset<Object> getAdapterExtendedData() {
		return adapterExtendedData;
	}

}