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
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregation;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregationOptions;
import org.locationtech.geowave.core.store.query.aggregate.CompositeAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation.FieldCountAggregation;

public class CompositeAggregationTest extends AbstractVectorAggregationTest {

  @Test
  public void testCompositeAggregation() {
    final List<SpatialTestType> features = generateFeatures();
    final CompositeAggregation<SpatialTestType> aggregation = new CompositeAggregation<>();
    aggregation.add(new FieldCountAggregation<>(null));
    aggregation.add(new FieldCountAggregation<>(new FieldNameParam(GEOMETRY_COLUMN)));
    aggregation.add(new FieldCountAggregation<>(new FieldNameParam(ALL_NULL_COLUMN)));
    aggregation.add(new FieldCountAggregation<>(new FieldNameParam(ODDS_NULL_COLUMN)));

    final List<Object> result = aggregateObjects(adapter, aggregation, features);
    assertEquals(4, result.size());
    assertTrue(result.get(0) instanceof Long);
    assertEquals(Long.valueOf(features.size()), result.get(0));
    assertTrue(result.get(1) instanceof Long);
    assertEquals(Long.valueOf(features.size()), result.get(1));
    assertTrue(result.get(2) instanceof Long);
    assertEquals(Long.valueOf(0L), result.get(2));
    assertTrue(result.get(3) instanceof Long);
    assertEquals(Long.valueOf((features.size() / 2) + 1), result.get(3));
  }

  @Test
  public void testCompositeAggregationWithBinning() {
    final List<SpatialTestType> features = generateFeatures();
    final CompositeAggregation<SpatialTestType> compositeAggregation = new CompositeAggregation<>();
    compositeAggregation.add(new FieldCountAggregation<>(null));
    compositeAggregation.add(new FieldCountAggregation<>(new FieldNameParam(GEOMETRY_COLUMN)));
    compositeAggregation.add(new FieldCountAggregation<>(new FieldNameParam(ALL_NULL_COLUMN)));
    compositeAggregation.add(new FieldCountAggregation<>(new FieldNameParam(ODDS_NULL_COLUMN)));
    final Aggregation<BinningAggregationOptions<PersistableList, SpatialTestType>, Map<ByteArray, List<Object>>, SpatialTestType> compositeBinningAggregation =
        new BinningAggregation<>(
            compositeAggregation,
            new SpatialFieldBinningStrategy<>(SpatialBinningType.S2, 4, true, GEOMETRY_COLUMN),
            -1);
    final Aggregation<BinningAggregationOptions<FieldNameParam, SpatialTestType>, Map<ByteArray, Long>, SpatialTestType> simpleBinningAggregation =
        new BinningAggregation<>(
            new FieldCountAggregation<>(new FieldNameParam(GEOMETRY_COLUMN)),
            new SpatialFieldBinningStrategy<>(SpatialBinningType.S2, 4, true, GEOMETRY_COLUMN),
            -1);
    final Map<ByteArray, List<Object>> compositeBinningResult =
        aggregateObjects(adapter, compositeBinningAggregation, features);
    final Map<ByteArray, Long> simpleBinningResult =
        aggregateObjects(adapter, simpleBinningAggregation, features);
    final List<Object> compositeResult = aggregateObjects(adapter, compositeAggregation, features);

    // first make sure each key for simple binning match the count of the corresponding composite
    // binning field
    assertEquals(simpleBinningResult.size(), compositeBinningResult.size());
    List<Object> aggregateBinningResult = null;
    for (final Entry<ByteArray, List<Object>> obj : compositeBinningResult.entrySet()) {
      final Long simpleResult = simpleBinningResult.get(obj.getKey());
      assertEquals(simpleResult, obj.getValue().get(1));
      if (aggregateBinningResult == null) {
        aggregateBinningResult = new ArrayList<>(obj.getValue());
      } else {
        aggregateBinningResult = compositeAggregation.merge(aggregateBinningResult, obj.getValue());
      }
    }
    // then make sure that aggregating the keys on the composite binning matches the non-binning
    // result
    assertEquals(compositeResult, aggregateBinningResult);
  }
}
