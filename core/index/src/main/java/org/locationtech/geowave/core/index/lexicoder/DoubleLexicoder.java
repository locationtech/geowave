/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.lexicoder;

import com.google.common.primitives.Longs;

/** A lexicoder for preserving the native Java sort order of Double values. */
public class DoubleLexicoder implements NumberLexicoder<Double> {

  @Override
  public byte[] toByteArray(final Double value) {
    long l = Double.doubleToRawLongBits(value);
    if (l < 0) {
      l = ~l;
    } else {
      l = l ^ 0x8000000000000000l;
    }
    return Longs.toByteArray(l);
  }

  @Override
  public Double fromByteArray(final byte[] bytes) {
    long l = Longs.fromByteArray(bytes);
    if (l < 0) {
      l = l ^ 0x8000000000000000l;
    } else {
      l = ~l;
    }
    return Double.longBitsToDouble(l);
  }

  @Override
  public Double getMinimumValue() {
    return -Double.MAX_VALUE;
  }

  @Override
  public Double getMaximumValue() {
    return Double.MAX_VALUE;
  }
}
