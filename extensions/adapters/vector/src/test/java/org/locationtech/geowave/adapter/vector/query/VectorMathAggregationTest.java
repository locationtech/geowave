/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import static org.junit.Assert.assertEquals;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorMaxAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorMinAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorSumAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.AbstractVectorAggregationTest;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.opengis.feature.simple.SimpleFeature;

public class VectorMathAggregationTest extends AbstractVectorAggregationTest {

  @Test
  public void testVectorMaxAggregation() {
    final List<SimpleFeature> features = generateFeatures();
    VectorMaxAggregation aggregation =
        new VectorMaxAggregation(new FieldNameParam(LATITUDE_COLUMN));
    BigDecimal result = aggregateObjects(aggregation, features);
    assertEquals(90L, result.longValue());

    aggregation = new VectorMaxAggregation(new FieldNameParam(LONGITUDE_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(180L, result.longValue());
  }

  @Test
  public void testVectorMinAggregation() {
    final List<SimpleFeature> features = generateFeatures();
    VectorMinAggregation aggregation =
        new VectorMinAggregation(new FieldNameParam(LATITUDE_COLUMN));
    BigDecimal result = aggregateObjects(aggregation, features);
    assertEquals(-90L, result.longValue());

    aggregation = new VectorMinAggregation(new FieldNameParam(LONGITUDE_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(-180L, result.longValue());
  }

  @Test
  public void testVectorSumAggregation() {
    final List<SimpleFeature> features = generateFeatures();
    VectorSumAggregation aggregation =
        new VectorSumAggregation(new FieldNameParam(LATITUDE_COLUMN));
    BigDecimal result = aggregateObjects(aggregation, features);
    assertEquals(0, result.longValue());

    aggregation = new VectorSumAggregation(new FieldNameParam(LONGITUDE_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(0, result.longValue());

    aggregation = new VectorSumAggregation(new FieldNameParam(VALUE_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(features.size() * (features.size() / 2), result.longValue());
  }

}
