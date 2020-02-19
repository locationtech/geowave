/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.data;

import org.junit.Assert;
import org.junit.Test;

public class NumericValueTest {

  private final double VALUE = 50;
  private final double DELTA = 1e-15;

  @Test
  public void testNumericValue() {

    final NumericValue numericValue = new NumericValue(VALUE);

    Assert.assertEquals(VALUE, numericValue.getMin(), DELTA);
    Assert.assertEquals(VALUE, numericValue.getMax(), DELTA);
    Assert.assertEquals(VALUE, numericValue.getCentroid(), DELTA);
    Assert.assertFalse(numericValue.isRange());
  }
}
