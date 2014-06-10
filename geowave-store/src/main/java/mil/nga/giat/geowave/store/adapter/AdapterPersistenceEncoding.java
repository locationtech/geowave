package mil.nga.giat.geowave.store.adapter;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.PersistenceEncoding;
import mil.nga.giat.geowave.store.data.PersistentDataset;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

/**
 * This is an implements of persistence encoding that also contains all of the
 * extended data values used to form the native type supported by this adapter.
 */
public class AdapterPersistenceEncoding extends
		PersistenceEncoding
{
	private final PersistentDataset<Object> adapterExtendedData;

	public AdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<? extends CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData ) {
		this(
				adapterId,
				dataId,
				0,
				commonData,
				adapterExtendedData);
	}

	public AdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final int duplicationCount,
			final PersistentDataset<? extends CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				duplicationCount,
				commonData);
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
