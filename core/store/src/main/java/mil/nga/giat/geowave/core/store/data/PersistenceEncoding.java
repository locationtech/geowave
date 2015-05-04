package mil.nga.giat.geowave.core.store.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import com.google.common.math.DoubleMath;

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
	private final static double DOUBLE_TOLERANCE = 1E-12d;

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

	private static class DimensionRangePair
	{
		DimensionField[] dimensions;
		NumericData[] dataPerDimension;

		DimensionRangePair(
				final DimensionField field,
				final NumericData data ) {
			dimensions = new DimensionField[] {
				field
			};
			dataPerDimension = new NumericData[] {
				data
			};
		}

		void add(
				final DimensionField field,
				final NumericData data ) {
			dimensions = ArrayUtils.add(
					dimensions,
					field);
			dataPerDimension = ArrayUtils.add(
					dataPerDimension,
					data);
		}
	}

	// Subclasses may want to override this behavior if the belief that the
	// index strategy is optimal
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
		final Map<ByteArrayId, DimensionRangePair> fieldsRangeData = new HashMap<ByteArrayId, DimensionRangePair>(
				dimensions.length);

		for (int d = 0; d < dimensions.length; d++) {
			final ByteArrayId fieldId = dimensions[d].getFieldId();
			final DimensionRangePair fieldData = fieldsRangeData.get(fieldId);
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
		for (final Entry<ByteArrayId, DimensionRangePair> entry : fieldsRangeData.entrySet()) {
			ok = ok && commonData.getValue(
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
		final MultiDimensionalNumericData boxRangeData = getNumericData(index.getIndexModel().getDimensions());
		final List<ByteArrayId> untrimmedResult = index.getIndexStrategy().getInsertionIds(
				boxRangeData);
		final int size = untrimmedResult.size();
		if (size > 3) { // need at least 4 quadrants in a quadtree to create a
			// concave shape where the mbr overlaps an area that the
			// underlying polygon doesn't
			final Iterator<ByteArrayId> it = untrimmedResult.iterator();
			while (it.hasNext()) {
				final ByteArrayId insertionId = it.next();
				// final MultiDimensionalNumericData md =
				// correctForNormalizationError(index.getIndexStrategy().getRangeForId(insertionId));
				// used to check the result of the index strategy
				if (LOGGER.isDebugEnabled() && checkCoverage(
						boxRangeData,
						index.getIndexStrategy().getRangeForId(
								insertionId))) {
					LOGGER.error("Index strategy produced an unmatching tile during encoding and storing an entry");
				}
				if (!overlaps(
						index.getIndexStrategy().getRangeForId(
								insertionId).getDataPerDimension(),
						index)) {
					it.remove();
				}
			}
		}
		return untrimmedResult;
	}

	public boolean isDeduplicationEnabled() {
		return true;
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
			final MultiDimensionalNumericData boxRangeData,
			final MultiDimensionalNumericData innerTile ) {
		for (int i = 0; i < boxRangeData.getDimensionCount(); i++) {
			final double i1 = innerTile.getDataPerDimension()[i].getMin();
			final double i2 = innerTile.getDataPerDimension()[i].getMax();
			final double j1 = boxRangeData.getDataPerDimension()[i].getMin();
			final double j2 = boxRangeData.getDataPerDimension()[i].getMax();
			final boolean overlaps = ((i1 < j2) || DoubleMath.fuzzyEquals(
					i1,
					j2,
					DOUBLE_TOLERANCE)) && ((i2 > j1) || DoubleMath.fuzzyEquals(
					i2,
					j1,
					DOUBLE_TOLERANCE));
			if (!overlaps) {
				return false;
			}
		}
		return true;
	}
}
