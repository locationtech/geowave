/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ByteArrayUtilsTest {

  @Test
  public void testSplit() {
    final ByteArray first = new ByteArray("first");
    final ByteArray second = new ByteArray("second");
    final byte[] combined =
        ByteArrayUtils.combineVariableLengthArrays(first.getBytes(), second.getBytes());
    final Pair<byte[], byte[]> split = ByteArrayUtils.splitVariableLengthArrays(combined);
    Assert.assertArrayEquals(first.getBytes(), split.getLeft());
    Assert.assertArrayEquals(second.getBytes(), split.getRight());
  }

  @Test
  public void testVariableLengthEncodeDecode() {
    testVariableLengthValue(0);
    testVariableLengthValue(123456L);
    testVariableLengthValue(-42L);
    testVariableLengthValue(Byte.MAX_VALUE);
    testVariableLengthValue(Byte.MIN_VALUE);
    testVariableLengthValue(Integer.MIN_VALUE);
    testVariableLengthValue(Integer.MAX_VALUE);
    testVariableLengthValue(Long.MAX_VALUE);
    testVariableLengthValue(Long.MIN_VALUE);
  }

  private void testVariableLengthValue(final long value) {
    final byte[] encoded = ByteArrayUtils.variableLengthEncode(value);
    final long result = ByteArrayUtils.variableLengthDecode(encoded);
    Assert.assertEquals(value, result);
  }
}
