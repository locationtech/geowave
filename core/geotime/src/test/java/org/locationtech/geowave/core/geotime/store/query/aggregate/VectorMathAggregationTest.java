/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import static org.junit.Assert.assertEquals;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.store.query.aggregate.FieldMaxAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldMinAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;

public class VectorMathAggregationTest extends AbstractVectorAggregationTest {

  @Test
  public void testVectorMaxAggregation() {
    final List<SpatialTestType> features = generateFeatures();
    FieldMaxAggregation<SpatialTestType> aggregation =
        new FieldMaxAggregation<>(new FieldNameParam(LATITUDE_COLUMN));
    BigDecimal result = aggregateObjects(adapter, aggregation, features);
    assertEquals(90L, result.longValue());

    aggregation = new FieldMaxAggregation<>(new FieldNameParam(LONGITUDE_COLUMN));
    result = aggregateObjects(adapter, aggregation, features);
    assertEquals(180L, result.longValue());
  }

  @Test
  public void testVectorMinAggregation() {
    final List<SpatialTestType> features = generateFeatures();
    FieldMinAggregation<SpatialTestType> aggregation =
        new FieldMinAggregation<>(new FieldNameParam(LATITUDE_COLUMN));
    BigDecimal result = aggregateObjects(adapter, aggregation, features);
    assertEquals(-90L, result.longValue());

    aggregation = new FieldMinAggregation<>(new FieldNameParam(LONGITUDE_COLUMN));
    result = aggregateObjects(adapter, aggregation, features);
    assertEquals(-180L, result.longValue());
  }

  @Test
  public void testVectorSumAggregation() {
    final List<SpatialTestType> features = generateFeatures();
    FieldSumAggregation<SpatialTestType> aggregation =
        new FieldSumAggregation<>(new FieldNameParam(LATITUDE_COLUMN));
    BigDecimal result = aggregateObjects(adapter, aggregation, features);
    assertEquals(0, result.longValue());

    aggregation = new FieldSumAggregation<>(new FieldNameParam(LONGITUDE_COLUMN));
    result = aggregateObjects(adapter, aggregation, features);
    assertEquals(0, result.longValue());

    aggregation = new FieldSumAggregation<>(new FieldNameParam(VALUE_COLUMN));
    result = aggregateObjects(adapter, aggregation, features);
    assertEquals(features.size() * (features.size() / 2), result.longValue());
  }

}
