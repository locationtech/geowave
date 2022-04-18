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

/** Concrete implementation defining a numeric range associated with a space filling curve. */
public class NumericRange implements NumericData {
  /** */
  private static final long serialVersionUID = 1L;

  private double min;
  private double max;

  private boolean minInclusive;
  private boolean maxInclusive;

  public NumericRange() {}

  /**
   * Constructor used to create a IndexRange object
   *
   * @param min the minimum bounds of a unique index range
   * @param max the maximum bounds of a unique index range
   */
  public NumericRange(final double min, final double max) {
    this(min, max, true, true);
  }

  public NumericRange(
      final double min,
      final double max,
      final boolean minInclusive,
      final boolean maxInclusive) {
    this.min = min;
    this.max = max;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;
  }

  /** @return min the minimum bounds of a index range object */
  @Override
  public Double getMin() {
    return min;
  }

  /** @return max the maximum bounds of a index range object */
  @Override
  public Double getMax() {
    return max;
  }

  @Override
  public boolean isMinInclusive() {
    return minInclusive;
  }

  @Override
  public boolean isMaxInclusive() {
    return maxInclusive;
  }

  /** @return centroid the center of a unique index range object */
  @Override
  public Double getCentroid() {
    return (min + max) / 2;
  }

  /** Flag to determine if the object is a range */
  @Override
  public boolean isRange() {
    return true;
  }

  @Override
  public String toString() {
    return "NumericRange [min=" + min + ", max=" + max + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(max);
    result = (prime * result) + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(min);
    result = (prime * result) + (int) (temp ^ (temp >>> 32));
    result = (prime * result) + (minInclusive ? 1 : 0);
    result = (prime * result) + (maxInclusive ? 1 : 0);
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
    // changing this check will fail some unit tests.
    if (!NumericRange.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    final NumericRange other = (NumericRange) obj;
    return (Math.abs(max - other.max) < NumericValue.EPSILON)
        && (Math.abs(min - other.min) < NumericValue.EPSILON);
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buf = ByteBuffer.allocate(18);
    buf.putDouble(min);
    buf.putDouble(max);
    buf.put(minInclusive ? (byte) 1 : (byte) 0);
    buf.put(maxInclusive ? (byte) 1 : (byte) 0);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    min = buf.getDouble();
    max = buf.getDouble();
    if (buf.remaining() > 0) {
      minInclusive = buf.get() > 0;
      maxInclusive = buf.get() > 0;
    } else {
      minInclusive = true;
      maxInclusive = true;
    }
  }
}
