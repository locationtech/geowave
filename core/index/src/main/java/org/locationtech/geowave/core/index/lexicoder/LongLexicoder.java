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

/**
 * A lexicoder for signed integers (in the range from Long.MIN_VALUE to Long.MAX_VALUE). Does an
 * exclusive or on the most significant bit to invert the sign, so that lexicographic ordering of
 * the byte arrays matches the natural order of the numbers.
 *
 * <p> See Apache Accumulo (org.apache.accumulo.core.client.lexicoder.LongLexicoder)
 */
public class LongLexicoder implements NumberLexicoder<Long> {

  protected LongLexicoder() {}

  @Override
  public byte[] toByteArray(final Long value) {
    return Longs.toByteArray(lexicode(value));
  }

  @Override
  public Long fromByteArray(final byte[] bytes) {
    final long value = Longs.fromByteArray(bytes);
    return lexicode(value);
  }

  @Override
  public Long getMinimumValue() {
    return Long.MIN_VALUE;
  }

  @Override
  public Long getMaximumValue() {
    return Long.MAX_VALUE;
  }

  public Long lexicode(final Long value) {
    return value ^ 0x8000000000000000l;
  }
}
