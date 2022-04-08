/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

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
public class BasicTextDataset implements MultiDimensionalTextData {

  private TextData[] dataPerDimension;

  /** Open ended/unconstrained */
  public BasicTextDataset() {
    dataPerDimension = new TextData[0];
  }

  /**
   * Constructor used to create a new Basic Text Dataset object.
   *
   * @param dataPerDimension an array of text data objects
   */
  public BasicTextDataset(final TextData[] dataPerDimension) {
    this.dataPerDimension = dataPerDimension;
  }

  /** @return all of the maximum values (for each dimension) */
  @Override
  public String[] getMaxValuesPerDimension() {
    final TextData[] ranges = getDataPerDimension();
    final String[] maxPerDimension = new String[ranges.length];
    for (int d = 0; d < ranges.length; d++) {
      maxPerDimension[d] = ranges[d].getMax();
    }
    return maxPerDimension;
  }

  /** @return all of the minimum values (for each dimension) */
  @Override
  public String[] getMinValuesPerDimension() {
    final TextData[] ranges = getDataPerDimension();
    final String[] minPerDimension = new String[ranges.length];
    for (int d = 0; d < ranges.length; d++) {
      minPerDimension[d] = ranges[d].getMin();
    }
    return minPerDimension;
  }

  /** @return all of the centroid values (for each dimension) */
  @Override
  public String[] getCentroidPerDimension() {
    final TextData[] ranges = getDataPerDimension();
    final String[] centroid = new String[ranges.length];
    for (int d = 0; d < ranges.length; d++) {
      centroid[d] = ranges[d].getCentroid();
    }
    return centroid;
  }

  /** @return an array of NumericData objects */
  @Override
  public TextData[] getDataPerDimension() {
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
    final BasicTextDataset other = (BasicTextDataset) obj;
    if (!Arrays.equals(dataPerDimension, other.dataPerDimension)) {
      return false;
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    int totalBytes = VarintUtils.unsignedIntByteLength(dataPerDimension.length);
    final List<byte[]> serializedData = new ArrayList<>();
    for (final TextData data : dataPerDimension) {
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
    dataPerDimension = new TextData[numDimensions];
    for (int d = 0; d < numDimensions; d++) {
      final byte[] binary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      dataPerDimension[d] = (TextData) PersistenceUtils.fromBinary(binary);
    }
  }
}
