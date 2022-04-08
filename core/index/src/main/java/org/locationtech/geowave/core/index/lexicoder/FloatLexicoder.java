/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.lexicoder;

import com.google.common.primitives.Ints;

/** A lexicoder for preserving the native Java sort order of Float values. */
public class FloatLexicoder implements NumberLexicoder<Float> {

  @Override
  public byte[] toByteArray(final Float value) {
    int i = Float.floatToRawIntBits(value);
    if (i < 0) {
      i = ~i;
    } else {
      i = i ^ 0x80000000;
    }

    return Ints.toByteArray(i);
  }

  @Override
  public Float fromByteArray(final byte[] bytes) {
    int i = Ints.fromByteArray(bytes);
    if (i < 0) {
      i = i ^ 0x80000000;
    } else {
      i = ~i;
    }

    return Float.intBitsToFloat(i);
  }

  @Override
  public Float getMinimumValue() {
    return -Float.MAX_VALUE;
  }

  @Override
  public Float getMaximumValue() {
    return Float.MAX_VALUE;
  }
}
