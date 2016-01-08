package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is an implementation of persistence encoding that also contains all of
 * the extended data values used to form the native type supported by this
 * adapter. It does not contain any information about the entry in a particular
 * index and is used when writing an entry, prior to its existence in an index.
 */
public class AdapterPersistenceEncoding extends
		CommonIndexedPersistenceEncoding
{
	private final PersistentDataset<Object> adapterExtendedData;

	public AdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				null,
				0,
				commonData,
				new PersistentDataset<byte[]>()); // all data is identified by
													// the adapter, there is
													// inherently no unknown
													// data elements
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
