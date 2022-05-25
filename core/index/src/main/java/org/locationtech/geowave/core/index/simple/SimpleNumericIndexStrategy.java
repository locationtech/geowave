/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Coordinate;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRanges;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.lexicoder.NumberLexicoder;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple 1-dimensional NumericIndexStrategy that represents an index of signed integer values
 * (currently supports 16 bit, 32 bit, and 64 bit integers). The strategy doesn't use any binning.
 * The ids are simply the byte arrays of the value. This index strategy will not perform well for
 * inserting ranges because there will be too much replication of data.
 */
public abstract class SimpleNumericIndexStrategy<T extends Number> implements NumericIndexStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleNumericIndexStrategy.class);

  private final NumberLexicoder<T> lexicoder;
  private final NumericDimensionDefinition[] definitions;

  protected SimpleNumericIndexStrategy(final NumberLexicoder<T> lexicoder) {
    this(
        lexicoder,
        new NumericDimensionDefinition[] {
            new BasicDimensionDefinition(
                lexicoder.getMinimumValue().doubleValue(),
                lexicoder.getMaximumValue().doubleValue())});
  }

  protected SimpleNumericIndexStrategy(
      final NumberLexicoder<T> lexicoder,
      final NumericDimensionDefinition[] definitions) {
    this.lexicoder = lexicoder;
    this.definitions = definitions;
  }

  public NumberLexicoder<T> getLexicoder() {
    return lexicoder;
  }

  /**
   * Cast a double into the type T
   *
   * @param value a double value
   * @return the value represented as a T
   */
  protected abstract T cast(double value);

  /**
   * Checks whehter
   *
   * @return the value represented as a T
   */
  protected abstract boolean isInteger();

  /**
   * Always returns a single range since this is a 1-dimensional index. The sort-order of the bytes
   * is the same as the sort order of values, so an indexedRange can be represented by a single
   * contiguous ByteArrayRange. {@inheritDoc}
   */
  @Override
  public QueryRanges getQueryRanges(
      final MultiDimensionalNumericData indexedRange,
      final IndexMetaData... hints) {
    return getQueryRanges(indexedRange, -1, hints);
  }

  /**
   * Always returns a single range since this is a 1-dimensional index. The sort-order of the bytes
   * is the same as the sort order of values, so an indexedRange can be represented by a single
   * contiguous ByteArrayRange. {@inheritDoc}
   */
  @Override
  public QueryRanges getQueryRanges(
      final MultiDimensionalNumericData indexedRange,
      final int maxEstimatedRangeDecomposition,
      final IndexMetaData... hints) {
    final T min = cast(indexedRange.getDataPerDimension()[0].getMin());
    byte[] start = lexicoder.toByteArray(min);
    final T max =
        cast(
            isInteger() ? Math.ceil(indexedRange.getDataPerDimension()[0].getMax())
                : indexedRange.getMaxValuesPerDimension()[0]);
    byte[] end = lexicoder.toByteArray(max);
    if (!indexedRange.getDataPerDimension()[0].isMinInclusive()) {
      start = ByteArrayUtils.getNextPrefix(start);
    }
    if (!indexedRange.getDataPerDimension()[0].isMaxInclusive()) {
      end = ByteArrayUtils.getPreviousPrefix(end);
    }
    final ByteArrayRange range = new ByteArrayRange(start, end);
    final SinglePartitionQueryRanges partitionRange =
        new SinglePartitionQueryRanges(Collections.singletonList(range));
    return new QueryRanges(Collections.singletonList(partitionRange));
  }

  /**
   * Returns all of the insertion ids for the range. Since this index strategy doensn't use binning,
   * it will return the ByteArrayId of every value in the range (i.e. if you are storing a range
   * using this index strategy, your data will be replicated for every integer value in the range).
   *
   * <p> {@inheritDoc}
   */
  @Override
  public InsertionIds getInsertionIds(final MultiDimensionalNumericData indexedData) {
    return getInsertionIds(indexedData, -1);
  }

  /**
   * Returns all of the insertion ids for the range. Since this index strategy doensn't use binning,
   * it will return the ByteArrayId of every value in the range (i.e. if you are storing a range
   * using this index strategy, your data will be replicated for every integer value in the range).
   *
   * <p> {@inheritDoc}
   */
  @Override
  public InsertionIds getInsertionIds(
      final MultiDimensionalNumericData indexedData,
      final int maxEstimatedDuplicateIds) {
    if (indexedData.isEmpty()) {
      LOGGER.warn("Cannot index empty fields, skipping writing row to index '" + getId() + "'");
      return new InsertionIds();
    }
    final double min = indexedData.getMinValuesPerDimension()[0];
    final double max = indexedData.getMaxValuesPerDimension()[0];
    final List<byte[]> insertionIds = new ArrayList<>((int) (max - min) + 1);
    for (double i = min; i <= max; i++) {
      insertionIds.add(lexicoder.toByteArray(cast(i)));
    }
    return new InsertionIds(insertionIds);
  }

  @Override
  public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
    return definitions;
  }

  @Override
  public MultiDimensionalNumericData getRangeForId(
      final byte[] partitionKey,
      final byte[] sortKey) {
    final double value = lexicoder.fromByteArray(sortKey).doubleValue();
    final NumericData[] dataPerDimension = new NumericData[] {new NumericValue(value)};
    return new BasicNumericDataset(dataPerDimension);
  }

  @Override
  public MultiDimensionalCoordinates getCoordinatesPerDimension(
      final byte[] partitionKey,
      final byte[] sortKey) {
    return new MultiDimensionalCoordinates(
        null,
        new Coordinate[] {new Coordinate(lexicoder.fromByteArray(sortKey).longValue(), null)});
  }

  @Override
  public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
      final MultiDimensionalNumericData dataRange,
      final IndexMetaData... hints) {
    return null;
  }

  @Override
  public double[] getHighestPrecisionIdRangePerDimension() {
    return new double[] {1d};
  }

  @Override
  public String getId() {
    return StringUtils.intToString(hashCode());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(definitions);
    result = (prime * result) + ((lexicoder == null) ? 0 : lexicoder.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final SimpleNumericIndexStrategy<?> other = (SimpleNumericIndexStrategy<?>) obj;
    if (!Arrays.equals(definitions, other.definitions)) {
      return false;
    }
    if (lexicoder == null) {
      if (other.lexicoder != null) {
        return false;
      }
    } else if (!lexicoder.equals(other.lexicoder)) {
      return false;
    }
    return true;
  }

  @Override
  public List<IndexMetaData> createMetaData() {
    return Collections.emptyList();
  }

  @Override
  public int getPartitionKeyLength() {
    return 0;
  }

  @Override
  public byte[][] getInsertionPartitionKeys(final MultiDimensionalNumericData insertionData) {
    return null;
  }

  @Override
  public byte[][] getQueryPartitionKeys(
      final MultiDimensionalNumericData queryData,
      final IndexMetaData... hints) {
    return null;
  }

  @Override
  public byte[] toBinary() {
    return new byte[] {};
  }

  @Override
  public void fromBinary(final byte[] bytes) {}
}
