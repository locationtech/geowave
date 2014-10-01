package mil.nga.giat.geowave.store.data;

import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

/**
 * This class models all of the necessary information for persisting data in
 * Accumulo (following the common index model) and is used internally within
 * GeoWave as an intermediary object between the direct storage format and the
 * native data format. It is the responsibility of the data adapter to convert
 * to and from this object and the native object. It does not contain any
 * information about the entry in a particular index and is used when writing an
 * entry, prior to its existence in an index.
 */
public class PersistenceEncoding
{
	private final ByteArrayId adapterId;
	private final ByteArrayId dataId;
	private final PersistentDataset<? extends CommonIndexValue> commonData;

	public PersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<? extends CommonIndexValue> commonData ) {
		this.adapterId = adapterId;
		this.dataId = dataId;
		this.commonData = commonData;
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

	/**
	 * Given an index, convert this persistent encoding to a set of insertion
	 * IDs for that index
	 *
	 * @param index
	 *            the index
	 * @return The insertions IDs for this object in the index
	 */
	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public List<ByteArrayId> getInsertionIds(
			final Index index ) {
		return index.getIndexStrategy().getInsertionIds(
				getNumericData(index.getIndexModel().getDimensions()));
	}
}
