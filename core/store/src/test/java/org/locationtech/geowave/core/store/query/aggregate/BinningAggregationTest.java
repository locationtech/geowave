/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import java.util.Map;
import java.util.UUID;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BinningAggregationTest extends
    AbstractAggregationTest<PersistableList, Map<String, Long>, CommonIndexedPersistenceEncoding> {

  // place all entries into separate bins.
  private static final AggregationBinningStrategy<CommonIndexedPersistenceEncoding> randomBinStrategy =
      new AggregationBinningStrategy<CommonIndexedPersistenceEncoding>() {
        @Override
        public byte[] toBinary() {
          return new byte[0];
        }

        @Override
        public void fromBinary(byte[] bytes) {}

        @Override
        public String[] binEntry(CommonIndexedPersistenceEncoding entry) {
          return new String[] {UUID.randomUUID().toString()};
        }
      };

  @Test
  public void testAggregate() {
    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    agg.aggregate(null);
    Map<String, Long> result = agg.getResult();
    assertThat(result.size(), is(1));

    agg.aggregate(null);
    result = agg.getResult();
    assertThat(result.size(), is(2));

    agg.clearResult();

    agg.aggregate(null);
    result = agg.getResult();
    assertThat(result.size(), is(1));
  }

  @Test
  public void testResultSerialization() {
    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    agg.aggregate(null);
    agg.aggregate(null);
    Map<String, Long> result = agg.getResult();

    byte[] serResult = agg.resultToBinary(result);
    Map<String, Long> deserResult = agg.resultFromBinary(serResult);

    // must iterate through both in case one is simply a subset of the other.
    for (Map.Entry<String, Long> resEntry : result.entrySet()) {
      assertThat(resEntry.getValue(), is(deserResult.get(resEntry.getKey())));
    }
    for (Map.Entry<String, Long> deserEntry : result.entrySet()) {
      assertThat(deserEntry.getValue(), is(result.get(deserEntry.getKey())));
    }
  }

  @Test
  public void testMerge() {
    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    Map<String, Long> res1 = ImmutableMap.of("0", 3L, "1", 2L);
    Map<String, Long> res2 = ImmutableMap.of("0", 2L, "1", 3L);

    // relies on CountAggregation#merge, which adds the values.
    Map<String, Long> merged = agg.merge(res1, res2);
    assertThat(merged.get("0"), is(5L));
    assertThat(merged.get("1"), is(5L));
  }

  @Test
  public void testFullSerialization() {
    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    byte[] serialized = PersistenceUtils.toBinary(agg);
    BinningAggregationOptions<Persistable, CommonIndexedPersistenceEncoding> params =
        agg.getParameters();

    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> roundtrip =
        (BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding>) PersistenceUtils.fromBinary(
            serialized);
    roundtrip.setParameters(params);

    // ensure that roundtrip can still properly instantiate the objects that it needs to on the fly.
    Map<String, Long> res1 = ImmutableMap.of("0", 3L, "1", 2L);
    Map<String, Long> res2 = ImmutableMap.of("0", 2L, "1", 3L);
    Map<String, Long> merged = roundtrip.merge(res1, res2);
    assertThat(merged.get("0"), is(5L));
    assertThat(merged.get("1"), is(5L));

    roundtrip.aggregate(null);
    roundtrip.aggregate(null);
    roundtrip.aggregate(null);
    assertThat(roundtrip.getResult().size(), is(3));
  }

  @Test
  public void testMaxBins() {
    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);
    for (int i = 0; i < 12336; i++) {
      agg.aggregate(null);
    }
    assertThat(agg.getResult().size(), is(12336));

    BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> boundedAgg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, 12);
    for (int i = 0; i < 2000; i++) {
      boundedAgg.aggregate(null);
    }
    assertThat(boundedAgg.getResult().size(), is(12));
  }
}
