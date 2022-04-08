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
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.jts.geom.Envelope;

public class VectorBoundingBoxAggregationTest extends AbstractVectorAggregationTest {

  @Test
  public void testVectorCountAggregation() {
    final List<SpatialTestType> features = generateFeatures();
    VectorBoundingBoxAggregation<SpatialTestType> aggregation =
        new VectorBoundingBoxAggregation<>(null);
    final Envelope expected = new Envelope(-180, 180, -90, 90);
    Envelope result = aggregateObjects(adapter, aggregation, features);
    assertEquals(expected, result);

    aggregation = new VectorBoundingBoxAggregation<>(new FieldNameParam(GEOMETRY_COLUMN));
    result = aggregateObjects(adapter, aggregation, features);
    assertEquals(expected, result);
  }

}
