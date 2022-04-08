/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.xz;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition;

public class XZOrderSFCTest {

  @Test
  public void testIndex() {
    final Double[] values = {42d, 43d, 57d, 59d};
    // TODO Meaningful examination of results?
    Assert.assertNotNull(createSFC().getId(values));
  }

  @Test
  public void testRangeDecomposition() {
    final NumericRange longBounds = new NumericRange(19.0, 21.0);
    final NumericRange latBounds = new NumericRange(33.0, 34.0);
    final NumericData[] dataPerDimension = {longBounds, latBounds};
    final MultiDimensionalNumericData query = new BasicNumericDataset(dataPerDimension);
    // TODO Meaningful examination of results?
    Assert.assertNotNull(createSFC().decomposeRangeFully(query));
  }

  private XZOrderSFC createSFC() {
    final SFCDimensionDefinition[] dimensions =
        {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 32),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 32)};
    return new XZOrderSFC(dimensions);
  }
}
