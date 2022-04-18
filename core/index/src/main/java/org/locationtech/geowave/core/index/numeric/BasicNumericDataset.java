/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.numeric;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

/**
 * The Basic Index Result class creates an object associated with a generic query. This class can be
 * used when the dimensions and/or axis are generic.
 */
public class BasicNumericDataset implements MultiDimensionalNumericData {

  private NumericData[] dataPerDimension;

  /** Open ended/unconstrained */
  public BasicNumericDataset() {
    dataPerDimension = new NumericData[0];
  }

  /**
   * Constructor used to create a new Basic Numeric Dataset object.
   *
   * @param dataPerDimension an array of numeric data objects
   */
  public BasicNumericDataset(final NumericData[] dataPerDimension) {
    this.dataPerDimension = dataPerDimension;
  }

  /** @return all of the maximum values (for each dimension) */
  @Override
  public Double[] getMaxValuesPerDimension() {
    final NumericData[] ranges = getDataPerDimension();
    final Double[] maxPerDimension = new Double[ranges.length];
    for (int d = 0; d < ranges.length; d++) {
      maxPerDimension[d] = ranges[d].getMax();
    }
    return maxPerDimension;
  }

  /** @return all of the minimum values (for each dimension) */
  @Override
  public Double[] getMinValuesPerDimension() {
    final NumericData[] ranges = getDataPerDimension();
    final Double[] minPerDimension = new Double[ranges.length];
    for (int d = 0; d < ranges.length; d++) {
      minPerDimension[d] = ranges[d].getMin();
    }
    return minPerDimension;
  }

  /** @return all of the centroid values (for each dimension) */
  @Override
  public Double[] getCentroidPerDimension() {
    final NumericData[] ranges = getDataPerDimension();
    final Double[] centroid = new Double[ranges.length];
    for (int d = 0; d < ranges.length; d++) {
      centroid[d] = ranges[d].getCentroid();
    }
    return centroid;
  }

  /** @return an array of NumericData objects */
  @Override
  public NumericData[] getDataPerDimension() {
    return dataPerDimension;
  }

  /** @return the number of dimensions associated with this data set */
  @Override
  public int getDimensionCount() {
    return dataPerDimension.length;
  }

  @Override
  public boolean isEmpty() {
    if ((dataPerDimension == null) || (dataPerDimension.length == 0)) {
      return true;
    }
    return !Arrays.stream(dataPerDimension).noneMatch(d -> d == null);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(dataPerDimension);
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
    final BasicNumericDataset other = (BasicNumericDataset) obj;
    if (!Arrays.equals(dataPerDimension, other.dataPerDimension)) {
      return false;
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    int totalBytes = VarintUtils.unsignedIntByteLength(dataPerDimension.length);
    final List<byte[]> serializedData = new ArrayList<>();
    for (final NumericData data : dataPerDimension) {
      final byte[] binary = PersistenceUtils.toBinary(data);
      totalBytes += (binary.length + VarintUtils.unsignedIntByteLength(binary.length));
      serializedData.add(binary);
    }
    final ByteBuffer buf = ByteBuffer.allocate(totalBytes);
    VarintUtils.writeUnsignedInt(dataPerDimension.length, buf);
    for (final byte[] binary : serializedData) {
      VarintUtils.writeUnsignedInt(binary.length, buf);
      buf.put(binary);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int numDimensions = VarintUtils.readUnsignedInt(buf);
    dataPerDimension = new NumericData[numDimensions];
    for (int d = 0; d < numDimensions; d++) {
      final byte[] binary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      dataPerDimension[d] = (NumericData) PersistenceUtils.fromBinary(binary);
    }
  }
}
