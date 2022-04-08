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
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.numeric.NumericValue;

public class BasicNumericDatasetTest {

  private final double DELTA = 1e-15;

  private final BasicNumericDataset basicNumericDatasetRanges =
      new BasicNumericDataset(
          new NumericData[] {
              new NumericRange(10, 50),
              new NumericRange(25, 95),
              new NumericRange(-50, 50)});

  private final BasicNumericDataset basicNumericDatasetValues =
      new BasicNumericDataset(
          new NumericData[] {new NumericValue(25), new NumericValue(60), new NumericValue(0)});

  @Test
  public void testNumericRangesMinValues() {

    final int expectedCount = 3;
    final Double[] expectedMinValues = new Double[] {10d, 25d, -50d};
    final Double[] mins = basicNumericDatasetRanges.getMinValuesPerDimension();

    Assert.assertEquals(expectedCount, basicNumericDatasetRanges.getDimensionCount());

    for (int i = 0; i < basicNumericDatasetRanges.getDimensionCount(); i++) {
      Assert.assertEquals(expectedMinValues[i], mins[i], DELTA);
    }
  }

  @Test
  public void testNumericRangesMaxValues() {

    final int expectedCount = 3;
    final Double[] expectedMaxValues = new Double[] {50d, 95d, 50d};
    final Double[] max = basicNumericDatasetRanges.getMaxValuesPerDimension();

    Assert.assertEquals(expectedCount, basicNumericDatasetRanges.getDimensionCount());

    for (int i = 0; i < basicNumericDatasetRanges.getDimensionCount(); i++) {
      Assert.assertEquals(expectedMaxValues[i], max[i], DELTA);
    }
  }

  @Test
  public void testNumericRangesCentroidValues() {

    final int expectedCount = 3;
    final Double[] expectedCentroidValues = new Double[] {30d, 60d, 0d};
    final Double[] centroid = basicNumericDatasetRanges.getCentroidPerDimension();

    Assert.assertEquals(expectedCount, basicNumericDatasetRanges.getDimensionCount());

    for (int i = 0; i < basicNumericDatasetRanges.getDimensionCount(); i++) {
      Assert.assertEquals(expectedCentroidValues[i], centroid[i], DELTA);
    }
  }

  @Test
  public void testNumericValuesAllValues() {

    final int expectedCount = 3;

    final double[] expectedValues = new double[] {25, 60, 0};

    final Double[] mins = basicNumericDatasetValues.getMinValuesPerDimension();
    final Double[] max = basicNumericDatasetValues.getMaxValuesPerDimension();
    final Double[] centroid = basicNumericDatasetValues.getCentroidPerDimension();

    Assert.assertEquals(expectedCount, basicNumericDatasetValues.getDimensionCount());

    for (int i = 0; i < basicNumericDatasetValues.getDimensionCount(); i++) {
      Assert.assertEquals(expectedValues[i], mins[i], DELTA);
      Assert.assertEquals(expectedValues[i], max[i], DELTA);
      Assert.assertEquals(expectedValues[i], centroid[i], DELTA);
    }
  }
}
