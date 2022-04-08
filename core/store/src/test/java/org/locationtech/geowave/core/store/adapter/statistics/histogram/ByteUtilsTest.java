/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics.histogram;

import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import org.junit.Test;

public class ByteUtilsTest {
  @Test
  public void test() {

    final double oneTwo = ByteUtils.toDouble("12".getBytes());
    final double oneOneTwo = ByteUtils.toDouble("112".getBytes());
    final double oneThree = ByteUtils.toDouble("13".getBytes());
    final double oneOneThree = ByteUtils.toDouble("113".getBytes());
    assertTrue(oneTwo > oneOneTwo);
    assertTrue(oneThree > oneTwo);
    assertTrue(oneOneTwo < oneOneThree);
    assertTrue(
        Arrays.equals(ByteUtils.toPaddedBytes("113".getBytes()), ByteUtils.toBytes(oneOneThree)));

    final double min = ByteUtils.toDouble(new byte[] {(byte) 0x00});
    final double mid = ByteUtils.toDouble(new byte[] {(byte) 0x8F});
    final double max = ByteUtils.toDouble(new byte[] {(byte) 0xFF});
    assertTrue(min < mid);
    assertTrue(mid < max);
    Double last = null;
    for (int i = 0; i < 256; i++) {
      final double current =
          ByteUtils.toDouble(
              new byte[] {
                  (byte) i,
                  (byte) i,
                  (byte) i,
                  (byte) i,
                  (byte) i,
                  (byte) i,
                  (byte) i,
                  (byte) i});
      if (last != null) {
        assertTrue(current > last);
      }
      last = current;
    }
  }
}
