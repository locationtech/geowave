/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;

/** Concrete implementation defining a text range. */
public class TextRange implements TextData {
  /** */
  private static final long serialVersionUID = 1L;

  private String min;
  private String max;

  private boolean minInclusive;
  private boolean maxInclusive;
  private boolean caseSensitive;
  private boolean reversed;

  public TextRange() {}

  /**
   * Constructor used to create a IndexRange object
   *
   * @param min the minimum bounds of a unique index range
   * @param max the maximum bounds of a unique index range
   */
  public TextRange(final String min, final String max) {
    this(min, max, true, true, true, false);
  }

  public TextRange(
      final String min,
      final String max,
      final boolean minInclusive,
      final boolean maxInclusive,
      final boolean caseSensitive,
      final boolean reversed) {
    this.min = min;
    this.max = max;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;
    this.caseSensitive = caseSensitive;
    this.reversed = reversed;
  }

  /** @return min the minimum bounds of a index range object */
  @Override
  public String getMin() {
    return min;
  }

  /** @return max the maximum bounds of a index range object */
  @Override
  public String getMax() {
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

  @Override
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public boolean isReversed() {
    return reversed;
  }

  /** @return centroid the center of a unique index range object */
  @Override
  public String getCentroid() {
    final int length = Math.min(min.length(), max.length());
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) ((min.charAt(i) + max.charAt(i)) / 2));
    }
    return sb.toString();
  }

  /** Flag to determine if the object is a range */
  @Override
  public boolean isRange() {
    return true;
  }

  @Override
  public String toString() {
    return "TextRange [min=" + min + ", max=" + max + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + min.hashCode();
    result = (prime * result) + max.hashCode();
    result = (prime * result) + (minInclusive ? 1 : 0);
    result = (prime * result) + (maxInclusive ? 1 : 0);
    result = (prime * result) + (caseSensitive ? 1 : 0);
    result = (prime * result) + (reversed ? 1 : 0);
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
    if (!TextRange.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    final TextRange other = (TextRange) obj;
    return min.equals(other.min)
        && max.equals(other.max)
        && (minInclusive == other.minInclusive)
        && (maxInclusive == other.maxInclusive)
        && (caseSensitive == other.caseSensitive)
        && (reversed == other.reversed);
  }

  @Override
  public byte[] toBinary() {
    final byte[] minBytes = StringUtils.stringToBinary(min);
    final byte[] maxBytes = StringUtils.stringToBinary(max);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(minBytes.length)
                + VarintUtils.unsignedIntByteLength(maxBytes.length)
                + minBytes.length
                + maxBytes.length
                + 4);
    VarintUtils.writeUnsignedInt(minBytes.length, buf);
    buf.put(minBytes);
    VarintUtils.writeUnsignedInt(maxBytes.length, buf);
    buf.put(maxBytes);
    buf.put(minInclusive ? (byte) 1 : (byte) 0);
    buf.put(maxInclusive ? (byte) 1 : (byte) 0);
    buf.put(caseSensitive ? (byte) 1 : (byte) 0);
    buf.put(reversed ? (byte) 1 : (byte) 0);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] minBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(minBytes);
    final byte[] maxBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(maxBytes);
    min = StringUtils.stringFromBinary(minBytes);
    max = StringUtils.stringFromBinary(maxBytes);
    minInclusive = buf.get() > 0;
    maxInclusive = buf.get() > 0;
    caseSensitive = buf.get() > 0;
    reversed = buf.get() > 0;
  }
}
