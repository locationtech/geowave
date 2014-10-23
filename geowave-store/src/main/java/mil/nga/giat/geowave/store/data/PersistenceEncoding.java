package mil.nga.giat.geowave.store.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
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
	private final static Logger LOGGER = Logger.getLogger(PersistenceEncoding.class);

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

	private class DimensionRangePair
	{
		DimensionField[] dimensions;
		NumericData[] dataPerDimension;

		DimensionRangePair(
				DimensionField field,
				NumericData data ) {
			dimensions = new DimensionField[] {
				field
			};
			dataPerDimension = new NumericData[] {
				data
			};
		}

		void add(
				DimensionField field,
				NumericData data ) {
			dimensions = ArrayUtils.add(
					dimensions,
					field);
			dataPerDimension = ArrayUtils.add(
					dataPerDimension,
					data);
		}
	}

	
	// Subclasses may want to override this behavior if the belief that the index strategy is optimal
	// to avoid the extra cost of checking the result
	protected boolean overlaps(
			final NumericData[] insertTileRange,
			final Index index ) {
		@SuppressWarnings("rawtypes")
		final DimensionField[] dimensions = index.getIndexModel().getDimensions();

		// Recall that each numeric data instance is extracted by a {@link
		// DimensionField}. More than one DimensionField
		// is associated with a {@link CommonIndexValue} entry (e.g. Lat/Long,
		// start/end). These DimensionField's share the
		// fieldId.
		// The infrastructure does not guarantee that CommonIndexValue can be
		// reconstructed fully from the NumericData.
		// However, provided in the correct order for interpretation, the
		// CommonIndexValue can use those numeric data items
		// to judge an overlap of range data.
		Map<ByteArrayId, DimensionRangePair> fieldsRangeData = new HashMap<ByteArrayId, DimensionRangePair>(
				dimensions.length);

		for (int d = 0; d < dimensions.length; d++) {
			final ByteArrayId fieldId = dimensions[d].getFieldId();
			DimensionRangePair fieldData = fieldsRangeData.get(fieldId);
			if (fieldData == null) {
				fieldsRangeData.put(
						fieldId,
						new DimensionRangePair(
								dimensions[d],
								insertTileRange[d]));
			}
			else {
				fieldData.add(
						dimensions[d],
						insertTileRange[d]);
			}
		}

		boolean ok = true;
		for (Entry<ByteArrayId, DimensionRangePair> entry : fieldsRangeData.entrySet()) {
			ok &= commonData.getValue(
					entry.getKey()).overlaps(
					entry.getValue().dimensions,
					entry.getValue().dataPerDimension);
		}
		return ok;
	}

	/**
	 * Given an index, convert this persistent encoding to a set of insertion
	 * IDs for that index
	 * 
	 * @param index
	 *            the index
	 * @return The insertions IDs for this object in the index
	 */
	public List<ByteArrayId> getInsertionIds(
			final Index index ) {
		MultiDimensionalNumericData boxRangeData = getNumericData(index.getIndexModel().getDimensions());
		List<ByteArrayId> untrimmedResult = index.getIndexStrategy().getInsertionIds(
				boxRangeData);
		final int size = untrimmedResult.size();
		if (size > 2) {
			Iterator<ByteArrayId> it = untrimmedResult.iterator();
			while (it.hasNext()) {
				ByteArrayId insertionId = it.next();
				MultiDimensionalNumericData md = correctForNormalizationError(index.getIndexStrategy().getRangeForId(
						insertionId));
				// used to check the result of the index strategy
				if (LOGGER.isDebugEnabled() && checkCoverage(
						boxRangeData,
						md)) {
					LOGGER.error("Index strategy produced an unmatching tile during encoding and storing an entry");
				}
				if (!overlaps(
						md.getDataPerDimension(),
						index)) it.remove();
			}
		}
		return untrimmedResult;
	}

	private MultiDimensionalNumericData correctForNormalizationError(
			MultiDimensionalNumericData boxRangeData ) {
		final NumericData[] currentDataSet = boxRangeData.getDataPerDimension();
		final NumericData[] dataPerDimension = new NumericData[currentDataSet.length];
		for (int d = 0; d < currentDataSet.length; d++) {
			dataPerDimension[d] = new NumericRange(
					currentDataSet[d].getMin() - 1E-12d,
					currentDataSet[d].getMax() + 1E-12d);
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}

	/**
	 * Tool can be used custom index strategies to check if the tiles actual
	 * intersect with the provided bounding box.
	 * 
	 * @param boxRangeData
	 * @param innerTile
	 * @return
	 */
	private boolean checkCoverage(
			MultiDimensionalNumericData boxRangeData,
			MultiDimensionalNumericData innerTile ) {
		for (int i = 0; i < boxRangeData.getDimensionCount(); i++) {
			double t0 = innerTile.getDataPerDimension()[i].getMax() - boxRangeData.getDataPerDimension()[i].getMin();
			double t1 = boxRangeData.getDataPerDimension()[i].getMax() - innerTile.getDataPerDimension()[i].getMin();
			if (Math.abs(t0 - t1) > (t0 + t1)) {
				return false;
			}
		}
		return true;
	}
}
