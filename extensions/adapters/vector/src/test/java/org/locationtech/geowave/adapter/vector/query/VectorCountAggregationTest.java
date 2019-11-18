/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import static org.junit.Assert.assertEquals;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.AbstractVectorAggregationTest;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.opengis.feature.simple.SimpleFeature;

public class VectorCountAggregationTest extends
    AbstractVectorAggregationTest<FieldNameParam, Long> {

  @Test
  public void testVectorCountAggregation() {
    List<SimpleFeature> features = generateFeatures();
    VectorCountAggregation aggregation = new VectorCountAggregation(null);
    Long result = aggregateObjects(aggregation, features);
    assertEquals(Long.valueOf(features.size()), result);

    aggregation = new VectorCountAggregation(new FieldNameParam(GEOMETRY_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(Long.valueOf(features.size()), result);

    aggregation = new VectorCountAggregation(new FieldNameParam(ALL_NULL_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(Long.valueOf(0L), result);

    aggregation = new VectorCountAggregation(new FieldNameParam(ODDS_NULL_COLUMN));
    result = aggregateObjects(aggregation, features);
    assertEquals(Long.valueOf(features.size() / 2 + 1), result);
  }

}
