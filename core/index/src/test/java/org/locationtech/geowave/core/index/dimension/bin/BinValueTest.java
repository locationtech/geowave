/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.dimension.bin;

import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class BinValueTest {

  final double BIN_VALUE = 100;
  private final double DELTA = 1e-15;

  @Test
  public void testBinValue() {

    final int binIdValue = 2;
    final byte[] binID = ByteBuffer.allocate(4).putInt(binIdValue).array();

    final BinValue binValue = new BinValue(binID, BIN_VALUE);

    Assert.assertEquals(BIN_VALUE, binValue.getNormalizedValue(), DELTA);
  }
}
