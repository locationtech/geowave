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
import org.locationtech.geowave.core.index.persist.Persistable;

public class MultiDimensionalCoordinateRangesArray implements Persistable {
  private MultiDimensionalCoordinateRanges[] rangesArray;

  public MultiDimensionalCoordinateRangesArray() {}

  public MultiDimensionalCoordinateRangesArray(
      final MultiDimensionalCoordinateRanges[] rangesArray) {
    this.rangesArray = rangesArray;
  }

  public MultiDimensionalCoordinateRanges[] getRangesArray() {
    return rangesArray;
  }

  @Override
  public byte[] toBinary() {
    final byte[][] rangesBinaries = new byte[rangesArray.length][];
    int binaryLength = VarintUtils.unsignedIntByteLength(rangesBinaries.length);
    for (int i = 0; i < rangesArray.length; i++) {
      rangesBinaries[i] = rangesArray[i].toBinary();
      binaryLength +=
          (VarintUtils.unsignedIntByteLength(rangesBinaries[i].length) + rangesBinaries[i].length);
    }
    final ByteBuffer buf = ByteBuffer.allocate(binaryLength);
    VarintUtils.writeUnsignedInt(rangesBinaries.length, buf);
    for (final byte[] rangesBinary : rangesBinaries) {
      VarintUtils.writeUnsignedInt(rangesBinary.length, buf);
      buf.put(rangesBinary);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    rangesArray = new MultiDimensionalCoordinateRanges[VarintUtils.readUnsignedInt(buf)];
    for (int i = 0; i < rangesArray.length; i++) {
      final byte[] rangesBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      rangesArray[i] = new MultiDimensionalCoordinateRanges();
      rangesArray[i].fromBinary(rangesBinary);
    }
  }

  public static class ArrayOfArrays implements Persistable {
    private MultiDimensionalCoordinateRangesArray[] coordinateArrays;

    public ArrayOfArrays() {}

    public ArrayOfArrays(final MultiDimensionalCoordinateRangesArray[] coordinateArrays) {
      this.coordinateArrays = coordinateArrays;
    }

    public MultiDimensionalCoordinateRangesArray[] getCoordinateArrays() {
      return coordinateArrays;
    }

    @Override
    public byte[] toBinary() {
      final byte[][] rangesBinaries = new byte[coordinateArrays.length][];
      int binaryLength = VarintUtils.unsignedIntByteLength(rangesBinaries.length);
      for (int i = 0; i < coordinateArrays.length; i++) {
        rangesBinaries[i] = coordinateArrays[i].toBinary();
        binaryLength +=
            (VarintUtils.unsignedIntByteLength(rangesBinaries[i].length)
                + rangesBinaries[i].length);
      }
      final ByteBuffer buf = ByteBuffer.allocate(binaryLength);
      VarintUtils.writeUnsignedInt(rangesBinaries.length, buf);
      for (final byte[] rangesBinary : rangesBinaries) {
        VarintUtils.writeUnsignedInt(rangesBinary.length, buf);
        buf.put(rangesBinary);
      }
      return buf.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int coordinateArrayLength = VarintUtils.readUnsignedInt(buf);
      ByteArrayUtils.verifyBufferSize(buf, coordinateArrayLength);
      coordinateArrays = new MultiDimensionalCoordinateRangesArray[coordinateArrayLength];
      for (int i = 0; i < coordinateArrayLength; i++) {
        final byte[] rangesBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
        coordinateArrays[i] = new MultiDimensionalCoordinateRangesArray();
        coordinateArrays[i].fromBinary(rangesBinary);
      }
    }
  }
}
