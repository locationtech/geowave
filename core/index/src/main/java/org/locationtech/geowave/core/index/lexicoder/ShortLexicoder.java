/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.lexicoder;

import com.google.common.primitives.Shorts;

/**
 * A lexicoder for signed integers (in the range from Short.MIN_VALUE to Short.MAX_VALUE). Does an
 * exclusive or on the most significant bit to invert the sign, so that lexicographic ordering of
 * the byte arrays matches the natural order of the numbers.
 */
public class ShortLexicoder implements NumberLexicoder<Short> {

  protected ShortLexicoder() {}

  @Override
  public byte[] toByteArray(final Short value) {
    return Shorts.toByteArray((short) (value ^ 0x8000));
  }

  @Override
  public Short fromByteArray(final byte[] bytes) {
    final short value = Shorts.fromByteArray(bytes);
    return (short) (value ^ 0x8000);
  }

  @Override
  public Short getMinimumValue() {
    return Short.MIN_VALUE;
  }

  @Override
  public Short getMaximumValue() {
    return Short.MAX_VALUE;
  }
}
