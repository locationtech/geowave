package mil.nga.giat.geowave.core.store.data;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class models all of the necessary information for persisting data in the
 * data store (following the common index model) and is used internally within
 * GeoWave as an intermediary object between the direct storage format and the
 * native data format. It is the responsibility of the data adapter to convert
 * to and from this object and the native object. It does not contain any
 * information about the entry in a particular index and is used when writing an
 * entry, prior to its existence in an index.
 */
public class PersistenceEncoding<T>
{
	private final ByteArrayId adapterId;
	private final ByteArrayId dataId;
	protected final PersistentDataset<T> commonData;
	private final PersistentDataset<byte[]> unknownData;
	protected final static Logger LOGGER = LoggerFactory.getLogger(PersistenceEncoding.class);
	protected final static double DOUBLE_TOLERANCE = 1E-12d;

	public PersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<T> commonData,
			final PersistentDataset<byte[]> unknownData ) {
		this.adapterId = adapterId;
		this.dataId = dataId;
		this.commonData = commonData;
		this.unknownData = unknownData;
	}

	/**
	 * Return the data that has been persisted but not identified by a field
	 * reader
	 * 
	 * @return the unknown data that is yet to be identified by a field reader
	 */
	public PersistentDataset<byte[]> getUnknownData() {
		return unknownData;
	}

	/**
	 * Return the common index data that has been persisted
	 * 
	 * @return the common index data
	 */
	public PersistentDataset<T> getCommonData() {
		return commonData;
	}

	/**
	 * Return the data adapter ID
	 * 
	 * @return the adapter ID
	 */
	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	/**
	 * Return the data ID, data ID's should be unique per adapter
	 * 
	 * @return the data ID
	 */
	public ByteArrayId getDataId() {
		return dataId;
	}

	public boolean isDeduplicationEnabled() {
		return true;
	}

}
