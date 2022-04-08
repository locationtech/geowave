/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.data;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.numeric.NumericRange;

public class NumericRangeTest {

  private final double MINIMUM = 20;
  private final double MAXIMUM = 100;
  private final double CENTROID = 60;
  private final double DELTA = 1e-15;

  @Test
  public void testNumericRangeValues() {

    final NumericRange numericRange = new NumericRange(MINIMUM, MAXIMUM);

    Assert.assertEquals(MINIMUM, numericRange.getMin(), DELTA);
    Assert.assertEquals(MAXIMUM, numericRange.getMax(), DELTA);
    Assert.assertEquals(CENTROID, numericRange.getCentroid(), DELTA);
    Assert.assertTrue(numericRange.isRange());
  }
}
