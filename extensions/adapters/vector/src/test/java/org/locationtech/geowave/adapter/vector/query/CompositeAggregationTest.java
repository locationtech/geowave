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
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.query.aggregation.CompositeVectorAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.AbstractVectorAggregationTest;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.opengis.feature.simple.SimpleFeature;

public class CompositeAggregationTest extends
    AbstractVectorAggregationTest<PersistableList, List<Object>> {

  @Test
  public void testCompositeAggregation() {
    List<SimpleFeature> features = generateFeatures();
    CompositeVectorAggregation aggregation = new CompositeVectorAggregation();
    aggregation.add(new VectorCountAggregation(null));
    aggregation.add(new VectorCountAggregation(new FieldNameParam(GEOMETRY_COLUMN)));
    aggregation.add(new VectorCountAggregation(new FieldNameParam(ALL_NULL_COLUMN)));
    aggregation.add(new VectorCountAggregation(new FieldNameParam(ODDS_NULL_COLUMN)));

    List<Object> result = aggregateObjects(aggregation, features);
    assertEquals(4, result.size());
    assertTrue(result.get(0) instanceof Long);
    assertEquals(Long.valueOf(features.size()), result.get(0));
    assertTrue(result.get(0) instanceof Long);
    assertEquals(Long.valueOf(features.size()), result.get(1));
    assertTrue(result.get(0) instanceof Long);
    assertEquals(Long.valueOf(0L), result.get(2));
    assertTrue(result.get(0) instanceof Long);
    assertEquals(Long.valueOf(features.size() / 2 + 1), result.get(3));
  }

}
