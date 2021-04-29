/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.BinningStrategy;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.google.common.collect.ImmutableMap;

public class BinningAggregationTest extends
    AbstractAggregationTest<PersistableList, Map<String, Long>, CommonIndexedPersistenceEncoding> {

  // place all entries into separate bins.
  private static final BinningStrategy randomBinStrategy = new BinningStrategy() {
    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public <T> ByteArray[] getBins(
        final DataTypeAdapter<T> type,
        final T entry,
        final GeoWaveRow... rows) {
      return new ByteArray[] {new ByteArray(UUID.randomUUID().toString())};
    }
  };

  @Test
  public void testAggregate() {
    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    agg.aggregate(null, null);
    Map<ByteArray, Long> result = agg.getResult();
    assertThat(result.size(), is(1));

    agg.aggregate(null, null);
    result = agg.getResult();
    assertThat(result.size(), is(2));

    agg.clearResult();

    agg.aggregate(null, null);
    result = agg.getResult();
    assertThat(result.size(), is(1));
  }

  @Test
  public void testResultSerialization() {
    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    agg.aggregate(null, null);
    agg.aggregate(null, null);
    final Map<ByteArray, Long> result = agg.getResult();

    final byte[] serResult = agg.resultToBinary(result);
    final Map<ByteArray, Long> deserResult = agg.resultFromBinary(serResult);

    // must iterate through both in case one is simply a subset of the other.
    for (final Map.Entry<ByteArray, Long> resEntry : result.entrySet()) {
      assertThat(resEntry.getValue(), is(deserResult.get(resEntry.getKey())));
    }
    for (final Map.Entry<ByteArray, Long> deserEntry : result.entrySet()) {
      assertThat(deserEntry.getValue(), is(result.get(deserEntry.getKey())));
    }
  }

  @Test
  public void testMerge() {
    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    final Map<ByteArray, Long> res1 =
        ImmutableMap.of(new ByteArray("0"), 3L, new ByteArray("1"), 2L);
    final Map<ByteArray, Long> res2 =
        ImmutableMap.of(new ByteArray("0"), 2L, new ByteArray("1"), 3L);

    // relies on CountAggregation#merge, which adds the values.
    final Map<ByteArray, Long> merged = agg.merge(res1, res2);
    assertThat(merged.get(new ByteArray("0")), is(5L));
    assertThat(merged.get(new ByteArray("1")), is(5L));
  }

  @Test
  public void testFullSerialization() {
    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);

    final byte[] serialized = PersistenceUtils.toBinary(agg);
    final BinningAggregationOptions<Persistable, CommonIndexedPersistenceEncoding> params =
        agg.getParameters();

    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> roundtrip =
        (BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding>) PersistenceUtils.fromBinary(
            serialized);
    roundtrip.setParameters(params);

    // ensure that roundtrip can still properly instantiate the objects that it needs to on the fly.
    final Map<ByteArray, Long> res1 =
        ImmutableMap.of(new ByteArray("0"), 3L, new ByteArray("1"), 2L);
    final Map<ByteArray, Long> res2 =
        ImmutableMap.of(new ByteArray("0"), 2L, new ByteArray("1"), 3L);
    final Map<ByteArray, Long> merged = roundtrip.merge(res1, res2);
    assertThat(merged.get(new ByteArray("0")), is(5L));
    assertThat(merged.get(new ByteArray("1")), is(5L));

    roundtrip.aggregate(null, null);
    roundtrip.aggregate(null, null);
    roundtrip.aggregate(null, null);
    assertThat(roundtrip.getResult().size(), is(3));
  }

  @Test
  public void testMaxBins() {
    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> agg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, -1);
    for (int i = 0; i < 12336; i++) {
      agg.aggregate(null, null);
    }
    assertThat(agg.getResult().size(), is(12336));

    final BinningAggregation<Persistable, Long, CommonIndexedPersistenceEncoding> boundedAgg =
        new BinningAggregation<>(new CountAggregation(), randomBinStrategy, 12);
    for (int i = 0; i < 2000; i++) {
      boundedAgg.aggregate(null, null);
    }
    assertThat(boundedAgg.getResult().size(), is(12));
  }
}
