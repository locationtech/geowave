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

/**
 * Concrete implementation defining a single text value.
 */
public class TextValue implements TextData {
  /** */
  private static final long serialVersionUID = 1L;

  private String value;

  private boolean caseSensitive;
  private boolean reversed;

  public TextValue() {}

  /**
   * Constructor used to create a new TextValue object
   *
   * @param value the particular text value
   */
  public TextValue(final String value, final boolean caseSensitive, final boolean reversed) {
    this.value = value;
    this.caseSensitive = caseSensitive;
    this.reversed = reversed;
  }

  /** @return value the value of a text value object */
  @Override
  public String getMin() {
    return value;
  }

  /** @return value the value of a text value object */
  @Override
  public String getMax() {
    return value;
  }

  @Override
  public boolean isMinInclusive() {
    return true;
  }

  @Override
  public boolean isMaxInclusive() {
    return true;
  }

  @Override
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public boolean isReversed() {
    return reversed;
  }

  /** @return value the value of a text value object */
  @Override
  public String getCentroid() {
    return value;
  }

  /** Determines if this object is a range or not */
  @Override
  public boolean isRange() {
    return false;
  }

  @Override
  public String toString() {
    return "TextRange [value=" + value + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + value.hashCode();
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
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TextValue other = (TextValue) obj;
    return value.equals(other.value);
  }

  @Override
  public byte[] toBinary() {
    final byte[] valueBytes = StringUtils.stringToBinary(value);
    final ByteBuffer buf = ByteBuffer.allocate(valueBytes.length + 2);
    buf.put(valueBytes);
    buf.put(caseSensitive ? (byte) 1 : (byte) 0);
    buf.put(reversed ? (byte) 1 : (byte) 0);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] valueBytes = new byte[buf.remaining() - 2];
    buf.get(valueBytes);
    value = StringUtils.stringFromBinary(valueBytes);
    caseSensitive = buf.get() > 0;
    reversed = buf.get() > 0;
  }
}
