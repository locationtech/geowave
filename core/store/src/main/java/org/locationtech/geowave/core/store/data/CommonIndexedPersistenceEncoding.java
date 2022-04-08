/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

/**
 * This class models all of the necessary information for persisting data in Accumulo (following the
 * common index model) and is used internally within GeoWave as an intermediary object between the
 * direct storage format and the native data format. It also contains information about the
 * persisted object within a particular index such as the insertion ID in the index and the number
 * of duplicates for this entry in the index, and is used when reading data from the index.
 */
public class CommonIndexedPersistenceEncoding extends IndexedPersistenceEncoding<Object> {

  public CommonIndexedPersistenceEncoding(
      final short internalAdapterId,
      final byte[] dataId,
      final byte[] insertionPartitionKey,
      final byte[] insertionSortKey,
      final int duplicateCount,
      final PersistentDataset<Object> commonData,
      final PersistentDataset<byte[]> unknownData) {
    super(
        internalAdapterId,
        dataId,
        insertionPartitionKey,
        insertionSortKey,
        duplicateCount,
        commonData,
        unknownData);
  }

  /**
   * Given an index, convert this persistent encoding to a set of insertion IDs for that index
   *
   * @param index the index
   * @return The insertions IDs for this object in the index
   */
  public InsertionIds getInsertionIds(final Index index) {
    final MultiDimensionalNumericData boxRangeData =
        getNumericData(index.getIndexModel().getDimensions());
    return index.getIndexStrategy().getInsertionIds(boxRangeData);
  }

  /**
   * Given an ordered set of dimensions, convert this persistent encoding common index data into a
   * MultiDimensionalNumericData object that can then be used by the Index
   *
   * @param dimensions the ordered set of dimensions
   * @return the numeric data
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public MultiDimensionalNumericData getNumericData(final NumericDimensionField[] dimensions) {
    final NumericData[] dataPerDimension = new NumericData[dimensions.length];
    for (int d = 0; d < dimensions.length; d++) {
      final Object val = getCommonData().getValue(dimensions[d].getFieldName());
      if (val != null) {
        dataPerDimension[d] = dimensions[d].getNumericData(val);
      }
    }
    return new BasicNumericDataset(dataPerDimension);
  }
}
