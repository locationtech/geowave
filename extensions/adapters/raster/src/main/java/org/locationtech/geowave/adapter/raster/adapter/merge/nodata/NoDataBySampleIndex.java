/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.merge.nodata;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.locationtech.geowave.core.index.VarintUtils;

public class NoDataBySampleIndex implements NoDataMetadata {
  private Set<SampleIndex> noDataIndexSet;

  public NoDataBySampleIndex() {
    super();
  }

  public NoDataBySampleIndex(final Set<SampleIndex> noDataIndexSet) {
    this.noDataIndexSet = noDataIndexSet;
  }

  @Override
  public byte[] toBinary() {
    int byteLength = 0;
    for (final SampleIndex i : noDataIndexSet) {
      byteLength +=
          VarintUtils.unsignedIntByteLength(i.getX())
              + VarintUtils.unsignedIntByteLength(i.getY())
              + VarintUtils.unsignedIntByteLength(i.getBand());
    }
    byteLength += VarintUtils.unsignedIntByteLength(noDataIndexSet.size());
    final ByteBuffer buf = ByteBuffer.allocate(byteLength);
    VarintUtils.writeUnsignedInt(noDataIndexSet.size(), buf);
    for (final SampleIndex i : noDataIndexSet) {
      VarintUtils.writeUnsignedInt(i.getX(), buf);
      VarintUtils.writeUnsignedInt(i.getY(), buf);
      VarintUtils.writeUnsignedInt(i.getBand(), buf);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int size = VarintUtils.readUnsignedInt(buf);
    noDataIndexSet = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      final int x = VarintUtils.readUnsignedInt(buf);
      final int y = VarintUtils.readUnsignedInt(buf);
      final int b = VarintUtils.readUnsignedInt(buf);
      noDataIndexSet.add(new SampleIndex(x, y, b));
    }
  }

  @Override
  public boolean isNoData(final SampleIndex index, final double sampleValue) {
    return noDataIndexSet.contains(index);
  }

  @Override
  public Set<SampleIndex> getNoDataIndices() {
    return noDataIndexSet;
  }
}
