/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class CustomSecondaryIndexIT {
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      options = {"enableSecondaryIndexing=true"},
      namespace = "BasicSecondaryIndexIT_dataIdxOnly")
  protected DataStorePluginOptions dataIdxOnlyDataStoreOptions;

  public static class TestCustomIndexStrategy implements
      CustomIndexStrategy<Pair<byte[], byte[]>, TestCustomConstraints> {

    public TestCustomIndexStrategy() {}

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public InsertionIds getInsertionIds(final Pair<byte[], byte[]> entry) {
      return new InsertionIds(Collections.singletonList(entry.getValue()));
    }

    @Override
    public QueryRanges getQueryRanges(final TestCustomConstraints constraints) {
      final byte[] sortKey = StringUtils.stringToBinary(constraints.matchText());
      return new QueryRanges(new ByteArrayRange(sortKey, sortKey));
    }

    @Override
    public Class<TestCustomConstraints> getConstraintsClass() {
      return TestCustomConstraints.class;
    }

  }

  /**
   * This class serves as constraints for our UUID index strategy. Since we only need to query for
   * exact UUIDs, the constraints class is fairly straightforward. We only need a single UUID String
   * to use as our constraint.
   */
  public static class TestCustomConstraints implements Persistable {
    private String matchText;

    public TestCustomConstraints() {}

    public TestCustomConstraints(final String matchText) {
      this.matchText = matchText;
    }

    public String matchText() {
      return matchText;
    }

    /**
     * Serialize any data needed to persist this constraint.
     */
    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(matchText);
    }

    /**
     * Load the UUID constraint from binary.
     */
    @Override
    public void fromBinary(final byte[] bytes) {
      matchText = StringUtils.stringFromBinary(bytes);
    }

  }

  @Test
  public void testDataIndexOnlyOnBinaryType() throws Exception {
    final DataStore dataStore = dataIdxOnlyDataStoreOptions.createDataStore();
    final BinaryDataAdapter adapter = new BinaryDataAdapter("testDataIndexOnlyOnBinaryType");
    final String customIndexName = "MatchTextIdx";
    dataStore.addType(adapter, new CustomIndex<>(new TestCustomIndexStrategy(), customIndexName));
    try (Writer<Pair<byte[], byte[]>> writer = dataStore.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < 9; i++) {
        writer.write(
            Pair.of(
                StringUtils.stringToBinary("abcdefghijk" + i),
                StringUtils.stringToBinary("abcdefghijk" + i)));
      }
    }

    for (int i = 0; i < 9; i++) {
      final String matchText = "abcdefghijk" + i;
      final byte[] id = StringUtils.stringToBinary(matchText);
      try (CloseableIterator<Pair<byte[], byte[]>> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIds(id)).build())) {
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(Arrays.equals(id, it.next().getRight()));
        Assert.assertFalse(it.hasNext());
      }
      try (CloseableIterator<Pair<byte[], byte[]>> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().constraints(
                  QueryBuilder.newBuilder().constraintsFactory().dataIdsByRange(id, id)).build())) {
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(Arrays.equals(id, it.next().getRight()));
        Assert.assertFalse(it.hasNext());
      }
      try (CloseableIterator<Pair<byte[], byte[]>> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().indexName(customIndexName).constraints(
                  QueryBuilder.newBuilder().constraintsFactory().customConstraints(
                      new TestCustomConstraints(matchText))).build())) {
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(Arrays.equals(id, it.next().getRight()));
        Assert.assertFalse(it.hasNext());
      }
    }
    TestUtils.deleteAll(dataIdxOnlyDataStoreOptions);
  }
}
