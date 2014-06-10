package mil.nga.giat.geowave.store.data;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

/**
 * This class models all of the necessary information for persisting data in
 * Accumulo (following the common index model) and is used internally within
 * GeoWave as an intermediary object between the direct storage format and the
 * native data format. It is the responsibility of the data adapter to convert
 * to and from this object and the native object.
 */
public class PersistenceEncoding
{
	private final ByteArrayId adapterId;
	private final ByteArrayId dataId;
	private final int duplicateCount;
	private final PersistentDataset<? extends CommonIndexValue> commonData;

	public PersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final int duplicateCount,
			final PersistentDataset<? extends CommonIndexValue> commonData ) {
		this.adapterId = adapterId;
		this.dataId = dataId;
		this.commonData = commonData;
		this.duplicateCount = duplicateCount;
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

	/**
	 * Return the common index data that has been persisted
	 * 
	 * @return the common index data
	 */
	public PersistentDataset<? extends CommonIndexValue> getCommonData() {
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

	/**
	 * Given an ordered set of dimensions, convert this persistent encoding
	 * common index data into a MultiDimensionalNumericData object that can then
	 * be used by the Index
	 * 
	 * @param dimensions
	 * @return
	 */
	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public MultiDimensionalNumericData getNumericData(
			final DimensionField[] dimensions ) {
		final NumericData[] dataPerDimension = new NumericData[dimensions.length];
		for (int d = 0; d < dimensions.length; d++) {
			dataPerDimension[d] = dimensions[d].getNumericData(commonData.getValue(dimensions[d].getFieldId()));
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}
}
