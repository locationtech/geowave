/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;

public class MultiDimensionalCoordinateRanges implements Persistable {
  // this is a generic placeholder for "tiers"
  private byte[] multiDimensionalId;
  private CoordinateRange[][] coordinateRangesPerDimension;

  public MultiDimensionalCoordinateRanges() {
    coordinateRangesPerDimension = new CoordinateRange[][] {};
  }

  public MultiDimensionalCoordinateRanges(
      final byte[] multiDimensionalPrefix,
      final CoordinateRange[][] coordinateRangesPerDimension) {
    multiDimensionalId = multiDimensionalPrefix;
    this.coordinateRangesPerDimension = coordinateRangesPerDimension;
  }

  public byte[] getMultiDimensionalId() {
    return multiDimensionalId;
  }

  public int getNumDimensions() {
    return coordinateRangesPerDimension.length;
  }

  public CoordinateRange[] getRangeForDimension(final int dimension) {
    return coordinateRangesPerDimension[dimension];
  }

  @Override
  public byte[] toBinary() {
    final List<byte[]> serializedRanges = new ArrayList<>();
    final int idLength = (multiDimensionalId == null ? 0 : multiDimensionalId.length);

    int byteLength = VarintUtils.unsignedIntByteLength(idLength) + idLength;
    byteLength += VarintUtils.unsignedIntByteLength(coordinateRangesPerDimension.length);
    final int[] numPerDimension = new int[getNumDimensions()];
    for (final int num : numPerDimension) {
      byteLength += VarintUtils.unsignedIntByteLength(num);
    }
    int d = 0;
    for (final CoordinateRange[] dim : coordinateRangesPerDimension) {
      numPerDimension[d++] = dim.length;
      for (final CoordinateRange range : dim) {
        final byte[] serializedRange = range.toBinary();
        byteLength +=
            (serializedRange.length + VarintUtils.unsignedIntByteLength(serializedRange.length));
        serializedRanges.add(serializedRange);
      }
    }
    final ByteBuffer buf = ByteBuffer.allocate(byteLength);
    VarintUtils.writeUnsignedInt(idLength, buf);
    if (idLength > 0) {
      buf.put(multiDimensionalId);
    }
    VarintUtils.writeUnsignedInt(coordinateRangesPerDimension.length, buf);
    for (final int num : numPerDimension) {
      VarintUtils.writeUnsignedInt(num, buf);
    }
    for (final byte[] serializedRange : serializedRanges) {
      VarintUtils.writeUnsignedInt(serializedRange.length, buf);
      buf.put(serializedRange);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int idLength = VarintUtils.readUnsignedInt(buf);
    if (idLength > 0) {
      multiDimensionalId = ByteArrayUtils.safeRead(buf, idLength);
    } else {
      multiDimensionalId = null;
    }
    coordinateRangesPerDimension = new CoordinateRange[VarintUtils.readUnsignedInt(buf)][];
    for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
      coordinateRangesPerDimension[d] = new CoordinateRange[VarintUtils.readUnsignedInt(buf)];
    }
    for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
      for (int i = 0; i < coordinateRangesPerDimension[d].length; i++) {
        final byte[] serializedRange =
            ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));

        coordinateRangesPerDimension[d][i] = new CoordinateRange();
        coordinateRangesPerDimension[d][i].fromBinary(serializedRange);
      }
    }
  }
}
