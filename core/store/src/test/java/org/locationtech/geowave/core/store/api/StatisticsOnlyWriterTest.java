/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.NumericMeanStatistic;

public class StatisticsOnlyWriterTest {

  private static final String TYPE_NAME = "TestData";

  private DataStore dataStore;

  public static class TestData {
    private String id;
    private String group;
    private double value;

    public TestData() {}

    public TestData(final String id, final String group, final double value) {
      this.id = id;
      this.group = group;
      this.value = value;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public String getGroup() {
      return group;
    }

    public void setGroup(final String group) {
      this.group = group;
    }

    public double getValue() {
      return value;
    }

    public void setValue(final double value) {
      this.value = value;
    }
  }

  private static final List<TestData> ENTRIES =
      Arrays.asList(
          new TestData("a", "even", 2.0),
          new TestData("b", "odd", 1.0),
          new TestData("c", "even", 4.0),
          new TestData("d", "odd", 3.0));

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, TestData.class, "id"),
        new NullIndex("index"));
  }

  // The in-memory store is cached statically per namespace, and these tests do not set one, so
  // anything left behind is visible to every other test in the JVM.
  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  @Test
  public void writesStatisticsWithoutStoringEntries() {
    final CountStatistic count = new CountStatistic(TYPE_NAME);
    count.setTag("count");
    final NumericMeanStatistic mean = new NumericMeanStatistic(TYPE_NAME, "value");
    mean.setTag("mean");
    dataStore.addEmptyStatistic(count, mean);

    try (StatisticsOnlyWriter<TestData> writer = dataStore.createStatisticsOnlyWriter(TYPE_NAME)) {
      assertNotNull(writer);
      writer.write(ENTRIES);
    }

    assertEquals(Long.valueOf(ENTRIES.size()), dataStore.getStatisticValue(count));
    assertEquals(2.5, dataStore.getStatisticValue(mean), 1e-9);

    // The whole point of the API: the entries themselves were never persisted.
    try (CloseableIterator<?> it =
        dataStore.query(QueryBuilder.newBuilder().addTypeName(TYPE_NAME).build())) {
      assertFalse("no entries should have been stored", it.hasNext());
    }
  }

  @Test
  public void appliesBinningStrategyPerBin() {
    final CountStatistic count = new CountStatistic(TYPE_NAME);
    count.setTag("binned-count");
    count.setBinningStrategy(new FieldValueBinningStrategy("group"));
    dataStore.addEmptyStatistic(count);

    try (StatisticsOnlyWriter<TestData> writer = dataStore.createStatisticsOnlyWriter(TYPE_NAME)) {
      writer.write(ENTRIES);
    }

    final Map<String, Long> countsByGroup = new HashMap<>();
    try (CloseableIterator<Pair<ByteArray, Long>> it = dataStore.getBinnedStatisticValues(count)) {
      while (it.hasNext()) {
        final Pair<ByteArray, Long> binned = it.next();
        countsByGroup.put(
            count.getBinningStrategy().binToString(binned.getKey()),
            binned.getValue());
      }
    }

    assertEquals(2, countsByGroup.size());
    assertEquals(Long.valueOf(2), countsByGroup.get("even"));
    assertEquals(Long.valueOf(2), countsByGroup.get("odd"));
  }

  @Test
  public void flushMakesStatisticsVisibleBeforeClose() {
    final CountStatistic count = new CountStatistic(TYPE_NAME);
    count.setTag("flushed");
    dataStore.addEmptyStatistic(count);

    try (StatisticsOnlyWriter<TestData> writer = dataStore.createStatisticsOnlyWriter(TYPE_NAME)) {
      writer.write(ENTRIES.get(0));
      writer.flush();
      assertEquals(Long.valueOf(1), dataStore.getStatisticValue(count));
    }
  }

  @Test
  public void rejectsWritesAfterClose() {
    dataStore.addEmptyStatistic(new CountStatistic(TYPE_NAME));
    final StatisticsOnlyWriter<TestData> writer = dataStore.createStatisticsOnlyWriter(TYPE_NAME);
    writer.close();
    try {
      writer.write(ENTRIES.get(0));
      fail("expected IllegalStateException");
    } catch (final IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void returnsNullForUnknownType() {
    assertNull(dataStore.createStatisticsOnlyWriter("DoesNotExist"));
  }
}
