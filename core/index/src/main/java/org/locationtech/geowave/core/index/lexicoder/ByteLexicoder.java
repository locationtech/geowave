/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.lexicoder;

/**
 * A lexicoder for signed values (in the range from Byte.MIN_VALUE to Byte.MAX_VALUE). Does an
 * exclusive or on the most significant bit to invert the sign, so that lexicographic ordering of
 * the byte arrays matches the natural order of the numbers.
 */
public class ByteLexicoder implements NumberLexicoder<Byte> {

  protected ByteLexicoder() {}

  @Override
  public byte[] toByteArray(final Byte value) {
    return new byte[] {((byte) (value ^ 0x80))};
  }

  @Override
  public Byte fromByteArray(final byte[] bytes) {
    return (byte) (bytes[0] ^ 0x80);
  }

  @Override
  public Byte getMinimumValue() {
    return Byte.MIN_VALUE;
  }

  @Override
  public Byte getMaximumValue() {
    return Byte.MAX_VALUE;
  }
}
