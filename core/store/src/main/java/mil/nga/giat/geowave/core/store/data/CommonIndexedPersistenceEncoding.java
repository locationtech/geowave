/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.math.DoubleMath;

/**
 * This class models all of the necessary information for persisting data in
 * Accumulo (following the common index model) and is used internally within
 * GeoWave as an intermediary object between the direct storage format and the
 * native data format. It also contains information about the persisted object
 * within a particular index such as the insertion ID in the index and the
 * number of duplicates for this entry in the index, and is used when reading
 * data from the index.
 */
public class CommonIndexedPersistenceEncoding extends
		IndexedPersistenceEncoding<CommonIndexValue>
{

	public CommonIndexedPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexInsertionId,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<byte[]> unknownData ) {
		super(
				adapterId,
				dataId,
				indexInsertionId,
				duplicateCount,
				commonData,
				unknownData);
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
			final PrimaryIndex index ) {
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
			final NumericDimensionField[] dimensions ) {
		final NumericData[] dataPerDimension = new NumericData[dimensions.length];
		for (int d = 0; d < dimensions.length; d++) {
			CommonIndexValue val = getCommonData().getValue(
					dimensions[d].getFieldId());
			if (val != null) {
				dataPerDimension[d] = dimensions[d].getNumericData(val);
			}
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}

	private static class DimensionRangePair
	{
		NumericDimensionField[] dimensions;
		NumericData[] dataPerDimension;

		DimensionRangePair(
				final NumericDimensionField field,
				final NumericData data ) {
			dimensions = new NumericDimensionField[] {
				field
			};
			dataPerDimension = new NumericData[] {
				data
			};
		}

		void add(
				final NumericDimensionField field,
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
			final PrimaryIndex index ) {
		@SuppressWarnings("rawtypes")
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		final NumericDimensionField[] fields = index.getIndexModel().getDimensions();
		Map<Class, NumericDimensionField> dimensionTypeToFieldMap = new HashMap<>();
		for (NumericDimensionField field : fields) {
			dimensionTypeToFieldMap.put(
					field.getBaseDefinition().getClass(),
					field);
		}

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
			Class baseDefinitionCls;
			if (dimensions[d] instanceof SFCDimensionDefinition) {
				baseDefinitionCls = ((SFCDimensionDefinition) dimensions[d]).getDimensionDefinition().getClass();
			}
			else {
				baseDefinitionCls = dimensions[d].getClass();
			}
			NumericDimensionField field = dimensionTypeToFieldMap.get(baseDefinitionCls);
			if (field != null) {
				final ByteArrayId fieldId = field.getFieldId();
				final DimensionRangePair fieldData = fieldsRangeData.get(fieldId);
				if (fieldData == null) {
					fieldsRangeData.put(
							fieldId,
							new DimensionRangePair(
									field,
									insertTileRange[d]));
				}
				else {
					fieldData.add(
							field,
							insertTileRange[d]);
				}
			}
		}

		for (final Entry<ByteArrayId, DimensionRangePair> entry : fieldsRangeData.entrySet()) {
			PersistentDataset<CommonIndexValue> commonData = getCommonData();
			if (commonData != null) {
				CommonIndexValue value = commonData.getValue(entry.getKey());
				if (value != null && !value.overlaps(
						entry.getValue().dimensions,
						entry.getValue().dataPerDimension)) {
					return false;
				}
			}
		}
		return true;
	}

}
