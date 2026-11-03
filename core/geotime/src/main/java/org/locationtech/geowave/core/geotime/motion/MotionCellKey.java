/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;

/** Identifies one motion vector: a time bucket, an H3 level and a cell at that level. */
public final class MotionCellKey implements Comparable<MotionCellKey> {
  private final long bucketStart;
  private final int level;
  private final long cell;

  public MotionCellKey(final long bucketStart, final int level, final long cell) {
    this.bucketStart = bucketStart;
    this.level = level;
    this.cell = cell;
  }

  public long getBucketStart() {
    return bucketStart;
  }

  public int getLevel() {
    return level;
  }

  public long getCell() {
    return cell;
  }

  /** The statistic bin of this key; see {@link #getBin(long, long)}. */
  public ByteArray getBin() {
    return getBin(bucketStart, cell);
  }

  /**
   * The composite bin of a time bucket followed by an H3 cell, each as an order-preserving 8-byte
   * long, so a bucket is a key prefix and cells sort by id within it. The level is implied by the
   * cell.
   */
  public static ByteArray getBin(final long bucketStart, final long cell) {
    return CompositeBinningStrategy.getBin(
        new ByteArray(Lexicoders.LONG.toByteArray(bucketStart)),
        new ByteArray(Lexicoders.LONG.toByteArray(cell)));
  }

  @Override
  public int compareTo(final MotionCellKey other) {
    int result = Long.compare(bucketStart, other.bucketStart);
    if (result == 0) {
      result = Integer.compare(level, other.level);
    }
    if (result == 0) {
      result = Long.compare(cell, other.cell);
    }
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MotionCellKey)) {
      return false;
    }
    final MotionCellKey other = (MotionCellKey) obj;
    return (bucketStart == other.bucketStart) && (level == other.level) && (cell == other.cell);
  }

  @Override
  public int hashCode() {
    return (31 * ((31 * Long.hashCode(bucketStart)) + level)) + Long.hashCode(cell);
  }

  @Override
  public String toString() {
    return bucketStart + "/" + level + "/" + Long.toHexString(cell);
  }
}
