/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;

/** Interface which defines a numeric index strategy. */
public interface NumericIndexStrategy extends
    SortedIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData>,
    PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> {

  /**
   * Return an integer coordinate in each dimension for the given partition and sort key plus a bin
   * ID if that dimension is continuous.
   *
   * @param partitionKey the partition key to determine the coordinates for
   * @param sortKey the sort key to determine the coordinates for
   * @return the integer coordinate that the given insertion ID represents and associated bin ID if
   *         that dimension is continuous
   */
  public MultiDimensionalCoordinates getCoordinatesPerDimension(
      byte[] partitionKey,
      byte[] sortKey);

  /**
   * Return an integer coordinate range in each dimension for the given data range plus a bin ID if
   * that dimension is continuous
   *
   * @param dataRange the range to determine the coordinates for
   * @param hints index hints
   * @return the integer coordinate ranges that the given data ID represents and associated bin IDs
   *         if a dimension is continuous
   */
  public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
      MultiDimensionalNumericData dataRange,
      IndexMetaData... hints);

  /**
   * Returns an array of dimension definitions that defines this index strategy, the array is in the
   * order that is expected within multidimensional numeric data that is passed to this index
   * strategy
   *
   * @return the ordered array of dimension definitions that represents this index strategy
   */
  public NumericDimensionDefinition[] getOrderedDimensionDefinitions();

  /**
   * * Get the range/size of a single insertion ID for each dimension at the highest precision
   * supported by this index strategy
   *
   * @return the range of a single insertion ID for each dimension
   */
  public double[] getHighestPrecisionIdRangePerDimension();

  /**
   * * Get the offset in bytes before the dimensional index. This can accounts for tier IDs and bin
   * IDs
   *
   * @return the byte offset prior to the dimensional index
   */
  @Override
  public int getPartitionKeyLength();
}
