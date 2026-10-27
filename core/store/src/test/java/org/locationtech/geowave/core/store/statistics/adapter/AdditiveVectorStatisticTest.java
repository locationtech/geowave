/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.adapter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.StatisticsOnlyWriterTest.TestData;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import com.beust.jcommander.ParameterException;

public class AdditiveVectorStatisticTest {
  private static final String TYPE_NAME = "AdditiveVectorData";
  private static final int SCHEMA = 7;
  private static final int COUNTS = 84;
  private static final int SUMS = 19;

  private DataStore dataStore;
  private DataStatisticsStore statisticsStore;

  @Before
  public void createStore() {
    final MemoryRequiredOptions options = new MemoryRequiredOptions();
    options.setGeoWaveNamespace("additive_vector_" + System.nanoTime());
    dataStore = DataStoreFactory.createDataStore(options);
    statisticsStore =
        new MemoryStoreFactoryFamily().getDataStatisticsStoreFactory().createStore(options);
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, TestData.class, "id"),
        new NullIndex("index"));
  }

  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  private static AdditiveVectorValue empty() {
    return new AdditiveVectorValue(null, SCHEMA, 1, COUNTS, SUMS);
  }

  private static AdditiveVectorValue random(final Random random, final int nonZeroCounts) {
    final AdditiveVectorValue value = empty();
    for (int i = 0; i < nonZeroCounts; i++) {
      value.addCount(random.nextInt(COUNTS), random.nextInt(2000) - 1000);
    }
    for (int i = 0; i < SUMS; i++) {
      if (random.nextBoolean()) {
        value.addSum(i, random.nextDouble() * 10000 - 5000);
      }
    }
    return value;
  }

  private static AdditiveVectorValue copy(final AdditiveVectorValue value) {
    final AdditiveVectorValue copy = empty();
    copy.add(value);
    return copy;
  }

  private static AdditiveVectorValue roundTrip(final AdditiveVectorValue value) {
    return (AdditiveVectorValue) PersistenceUtils.fromBinary(PersistenceUtils.toBinary(value));
  }

  private static void assertVectorEquals(
      final AdditiveVectorValue expected,
      final AdditiveVectorValue actual,
      final double relativeTolerance) {
    assertEquals(expected.getSchemaId(), actual.getSchemaId());
    assertEquals(expected.getSchemaVersion(), actual.getSchemaVersion());
    assertArrayEquals(expected.getCounts(), actual.getCounts());
    assertEquals(expected.getSumLength(), actual.getSumLength());
    for (int i = 0; i < expected.getSumLength(); i++) {
      assertEquals(
          expected.getSum(i),
          actual.getSum(i),
          Math.max(1e-9, Math.abs(expected.getSum(i)) * relativeTolerance));
    }
  }

  @Test
  public void testAdditionIsCommutativeAndAssociative() {
    final Random random = new Random(42);
    for (int trial = 0; trial < 200; trial++) {
      final AdditiveVectorValue a = random(random, 12);
      final AdditiveVectorValue b = random(random, 30);
      final AdditiveVectorValue c = random(random, 5);

      final AdditiveVectorValue ab = copy(a);
      ab.add(b);
      final AdditiveVectorValue ba = copy(b);
      ba.add(a);
      assertVectorEquals(ab, ba, 0);

      final AdditiveVectorValue abThenC = copy(ab);
      abThenC.add(c);
      final AdditiveVectorValue bc = copy(b);
      bc.add(c);
      final AdditiveVectorValue aThenBc = copy(a);
      aThenBc.add(bc);
      assertVectorEquals(abThenC, aThenBc, 1e-12);
    }
  }

  @Test
  public void testSubtractionInvertsAddition() {
    final Random random = new Random(7);
    for (int trial = 0; trial < 200; trial++) {
      final AdditiveVectorValue a = random(random, 20);
      final AdditiveVectorValue b = random(random, 20);
      final AdditiveVectorValue sum = copy(a);
      sum.add(b);
      sum.subtract(b);
      assertVectorEquals(a, sum, 1e-12);

      final AdditiveVectorValue difference = copy(a);
      difference.subtract(a);
      assertTrue(difference.isEmpty());
    }
  }

  @Test
  public void testMergeIsAddition() {
    final Random random = new Random(3);
    final AdditiveVectorValue a = random(random, 10);
    final AdditiveVectorValue b = random(random, 10);
    final AdditiveVectorValue merged = copy(a);
    merged.merge(b);
    final AdditiveVectorValue added = copy(a);
    added.add(b);
    assertVectorEquals(added, merged, 0);
  }

  @Test
  public void testUnsetValueIsIdentity() {
    final AdditiveVectorValue value = random(new Random(11), 10);
    final AdditiveVectorValue unset = new AdditiveVectorValue();
    unset.merge(value);
    assertVectorEquals(value, unset, 0);

    final AdditiveVectorValue unchanged = copy(value);
    unchanged.merge(new AdditiveVectorValue());
    assertVectorEquals(value, unchanged, 0);
  }

  @Test
  public void testMixedSchemasNeverThrowAndTheLargerIdWins() {
    final Random random = new Random(29);
    final AdditiveVectorValue smaller = random(random, 10);
    final AdditiveVectorValue larger = new AdditiveVectorValue(null, SCHEMA + 1, 1, COUNTS, SUMS);
    larger.addCount(3, 4);
    larger.addSum(2, 1.5);

    final AdditiveVectorValue smallerFirst = copy(smaller);
    smallerFirst.merge(larger);
    assertVectorEquals(larger, smallerFirst, 0);

    final AdditiveVectorValue largerFirst = copy(larger);
    largerFirst.merge(smaller);
    assertVectorEquals(larger, largerFirst, 0);

    final AdditiveVectorValue added = copy(smaller);
    added.add(larger);
    assertVectorEquals(larger, added, 0);
    final AdditiveVectorValue subtracted = copy(smaller);
    subtracted.subtract(larger);
    assertEquals(SCHEMA + 1, subtracted.getSchemaId());
    assertEquals(-4L, subtracted.getCount(3));
  }

  @Test
  public void testMergeIsAssociativeOverMixedSchemas() {
    final Random random = new Random(31);
    for (int trial = 0; trial < 200; trial++) {
      final AdditiveVectorValue[] values = new AdditiveVectorValue[3];
      for (int i = 0; i < values.length; i++) {
        values[i] = new AdditiveVectorValue(null, 1 + random.nextInt(3), 1, COUNTS, SUMS);
        values[i].addCount(random.nextInt(COUNTS), random.nextInt(100) + 1);
        values[i].addSum(random.nextInt(SUMS), random.nextDouble() * 100);
      }
      final AdditiveVectorValue left = new AdditiveVectorValue();
      left.merge(values[0]);
      left.merge(values[1]);
      left.merge(values[2]);
      final AdditiveVectorValue bc = new AdditiveVectorValue();
      bc.merge(values[1]);
      bc.merge(values[2]);
      final AdditiveVectorValue right = new AdditiveVectorValue();
      right.merge(values[0]);
      right.merge(bc);
      final AdditiveVectorValue reversed = new AdditiveVectorValue();
      reversed.merge(values[2]);
      reversed.merge(values[1]);
      reversed.merge(values[0]);
      assertVectorEquals(left, right, 1e-12);
      assertVectorEquals(left, reversed, 1e-12);
    }
  }

  @Test
  public void testSchemaIdAndLengthsAreValidated() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorStatistic(TYPE_NAME, 0, 1, COUNTS, SUMS));
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorStatistic(TYPE_NAME, -1, 1, COUNTS, SUMS));
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorStatistic(TYPE_NAME, SCHEMA, 1, -1, SUMS));
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorStatistic(TYPE_NAME, SCHEMA, 1, COUNTS, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorValue(null, 0, 1, COUNTS, SUMS));
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorValue(null, SCHEMA, 1, -1, SUMS));
    assertThrows(
        IllegalArgumentException.class,
        () -> new AdditiveVectorValue(null, SCHEMA, 1, COUNTS, -1));
    assertEquals(0, new AdditiveVectorValue().getSchemaId());
    assertEquals(0, new AdditiveVectorValue(null).getSchemaId());
  }

  @Test
  public void testReadBytesRejectsUnsetSchemaId() {
    final byte[] bytes =
        PersistenceUtils.toBinary(new AdditiveVectorStatistic(TYPE_NAME, 1, 1, 5, 3));
    // the schema id, version and lengths are the last four single-byte varints
    final int schemaIdOffset = bytes.length - 4;
    assertEquals(1, bytes[schemaIdOffset]);
    assertEquals(1, ((AdditiveVectorStatistic) PersistenceUtils.fromBinary(bytes)).getSchemaId());
    bytes[schemaIdOffset] = 0;
    assertThrows(IllegalArgumentException.class, () -> PersistenceUtils.fromBinary(bytes));
  }

  @Test
  public void testCommandLineValidators() {
    final AdditiveVectorStatistic.SchemaIdValidator schemaId =
        new AdditiveVectorStatistic.SchemaIdValidator();
    schemaId.validate("--schemaId", 1);
    assertThrows(ParameterException.class, () -> schemaId.validate("--schemaId", 0));
    assertThrows(ParameterException.class, () -> schemaId.validate("--schemaId", -3));
    final AdditiveVectorStatistic.LengthValidator length =
        new AdditiveVectorStatistic.LengthValidator();
    length.validate("--countLength", 0);
    assertThrows(ParameterException.class, () -> length.validate("--countLength", -1));
  }

  @Test
  public void testLaterVersionAppends() {
    final AdditiveVectorValue v1 = new AdditiveVectorValue(null, SCHEMA, 1, 3, 2);
    v1.addCount(2, 5);
    v1.addSum(1, 1.5);
    final AdditiveVectorValue v2 = new AdditiveVectorValue(null, SCHEMA, 2, 5, 3);
    v2.addCount(2, 1);
    v2.addCount(4, 9);
    v2.addSum(2, 2.5);

    v1.add(v2);
    assertEquals(2, v1.getSchemaVersion());
    assertArrayEquals(new long[] {0, 0, 6, 0, 9}, v1.getCounts());
    assertArrayEquals(new double[] {0, 1.5, 2.5}, v1.getSums(), 0);
    assertEquals(0L, v1.getCount(10));
    assertEquals(0.0, v1.getSum(10), 0);
  }

  @Test
  public void testSerializationRoundTrip() {
    final Random random = new Random(19);
    for (final int nonZeroCounts : new int[] {0, 1, 5, 40, 500}) {
      final AdditiveVectorValue value = random(random, nonZeroCounts);
      value.addCount(0, Long.MIN_VALUE / 2);
      value.addCount(COUNTS - 1, Long.MAX_VALUE / 2);
      final AdditiveVectorValue restored = roundTrip(value);
      // sums are stored as 32-bit floats
      assertVectorEquals(value, restored, 1e-6);
      assertVectorEquals(restored, roundTrip(restored), 0);
    }
  }

  @Test
  public void testSerializedSize() {
    // four 1-byte header varints and two 1-byte block headers; the persistable class id adds 2
    assertEquals(6, empty().toBinary().length);
    assertEquals(8, PersistenceUtils.toBinary(empty()).length);

    // a typical lane cell-hour: a dozen non-zero counts and ten non-zero sums
    final AdditiveVectorValue typical = empty();
    for (int i = 0; i < 12; i++) {
      typical.addCount(i * 7, 3 + i);
    }
    for (int i = 0; i < 10; i++) {
      typical.addSum(i * 2, 1234.5 * (i + 1));
    }
    assertTrue(typical.toBinary().length <= 120);

    // every position set: the dense encodings bound the size
    final AdditiveVectorValue full = empty();
    for (int i = 0; i < COUNTS; i++) {
      full.addCount(i, 100);
    }
    for (int i = 0; i < SUMS; i++) {
      full.addSum(i, 1e6);
    }
    assertTrue(full.toBinary().length <= 4 + 1 + (2 * COUNTS) + 1 + (Float.BYTES * SUMS) + 2);
  }

  @Test
  public void testStatisticRoundTrip() {
    final AdditiveVectorStatistic statistic =
        new AdditiveVectorStatistic(TYPE_NAME, SCHEMA, 3, COUNTS, SUMS);
    final AdditiveVectorStatistic restored =
        (AdditiveVectorStatistic) PersistenceUtils.fromBinary(PersistenceUtils.toBinary(statistic));
    assertEquals(SCHEMA, restored.getSchemaId());
    assertEquals(3, restored.getSchemaVersion());
    assertEquals(COUNTS, restored.getCountLength());
    assertEquals(SUMS, restored.getSumLength());
    final AdditiveVectorValue value = restored.createEmpty();
    assertEquals(SCHEMA, value.getSchemaId());
    assertEquals(COUNTS, value.getCountLength());
    assertEquals(SUMS, value.getSumLength());
    assertTrue(value.isEmpty());
  }

  @Test
  public void testIncorporatedValuesAddUpPerBin() {
    final AdditiveVectorStatistic statistic =
        new AdditiveVectorStatistic(TYPE_NAME, SCHEMA, 1, COUNTS, SUMS);
    statistic.setBinningStrategy(new FieldValueBinningStrategy("group"));
    dataStore.addEmptyStatistic(statistic);

    final Random random = new Random(23);
    final Map<ByteArray, AdditiveVectorValue> expected = new HashMap<>();
    for (int i = 0; i < 12; i++) {
      final ByteArray bin = new ByteArray("cell-" + (i % 3));
      final AdditiveVectorValue delta = random(random, 6);
      dataStore.incorporateStatisticValue(statistic, delta, bin);
      expected.computeIfAbsent(bin, b -> empty()).add(delta);
    }
    // a correction is written as a negated difference
    final ByteArray corrected = new ByteArray("cell-0");
    final AdditiveVectorValue correction = empty();
    correction.subtract(expected.get(corrected));
    dataStore.incorporateStatisticValue(statistic, correction, corrected);
    expected.get(corrected).add(correction);

    int bins = 0;
    try (CloseableIterator<Pair<ByteArray, AdditiveVectorValue>> it =
        dataStore.getBinnedStatisticValues(statistic, BinConstraints.allBins())) {
      while (it.hasNext()) {
        final Pair<ByteArray, AdditiveVectorValue> binned = it.next();
        if (!binned.getKey().equals(corrected)) {
          assertVectorEquals(expected.get(binned.getKey()), binned.getValue(), 1e-5);
        }
        bins++;
      }
    }
    assertEquals(3, bins);
    // each stored delta was rounded to float, so the canceled sums are only close to zero
    final AdditiveVectorValue canceled =
        dataStore.getStatisticValue(statistic, BinConstraints.of(corrected));
    assertArrayEquals(new long[COUNTS], canceled.getCounts());
    assertArrayEquals(new double[SUMS], canceled.getSums(), 1e-2);
    assertFalse(
        dataStore.getStatisticValue(
            statistic,
            BinConstraints.of(new ByteArray("cell-1"))).isEmpty());
  }

  private AdditiveVectorStatistic binnedStatistic() {
    final AdditiveVectorStatistic statistic =
        new AdditiveVectorStatistic(TYPE_NAME, 1, 1, COUNTS, SUMS);
    statistic.setBinningStrategy(new FieldValueBinningStrategy("group"));
    dataStore.addEmptyStatistic(statistic);
    return statistic;
  }

  private static AdditiveVectorValue valueOf(final int schemaId, final long count) {
    final AdditiveVectorValue value = new AdditiveVectorValue(null, schemaId, 1, COUNTS, SUMS);
    value.addCount(0, count);
    return value;
  }

  private long countOf(final AdditiveVectorStatistic statistic, final ByteArray bin) {
    return dataStore.getStatisticValue(statistic, BinConstraints.of(bin)).getCount(0);
  }

  @Test
  public void testValueOfAnotherSchemaIsRejectedAndNothingIsWritten() {
    final AdditiveVectorStatistic statistic = binnedStatistic();
    final ByteArray a = new ByteArray("a");
    final ByteArray b = new ByteArray("b");
    dataStore.incorporateStatisticValue(statistic, valueOf(1, 7), a);
    dataStore.incorporateStatisticValue(statistic, valueOf(1, 10), b);

    final IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> dataStore.incorporateStatisticValue(statistic, valueOf(2, 1), a));
    assertTrue(thrown.getMessage().contains("2") && thrown.getMessage().contains("1"));

    assertEquals(1, dataStore.getStatisticValue(statistic, BinConstraints.of(a)).getSchemaId());
    assertEquals(7L, countOf(statistic, a));
    assertEquals(10L, countOf(statistic, b));
    dataStore.incorporateStatisticValue(statistic, valueOf(1, 100), a);
    assertEquals(107L, countOf(statistic, a));
  }

  @Test
  public void testUnbinnedValueOfAnotherSchemaIsRejected() {
    final AdditiveVectorStatistic statistic =
        new AdditiveVectorStatistic(TYPE_NAME, 1, 1, COUNTS, SUMS);
    dataStore.addEmptyStatistic(statistic);
    dataStore.incorporateStatisticValue(statistic, valueOf(1, 7));
    assertThrows(
        IllegalArgumentException.class,
        () -> dataStore.incorporateStatisticValue(statistic, valueOf(2, 1)));
    assertEquals(1, dataStore.getStatisticValue(statistic).getSchemaId());
    assertEquals(7L, dataStore.getStatisticValue(statistic).getCount(0));
  }

  @Test
  public void testUnsetValueIsAccepted() {
    final AdditiveVectorStatistic statistic = binnedStatistic();
    final ByteArray a = new ByteArray("a");
    dataStore.incorporateStatisticValue(statistic, valueOf(1, 7), a);
    dataStore.incorporateStatisticValue(statistic, new AdditiveVectorValue(statistic), a);
    assertEquals(7L, countOf(statistic, a));
  }

  @Test
  public void testMergeStatsKeepsTheLargerSchemaOfDataAlreadyStored() {
    final AdditiveVectorStatistic statistic = binnedStatistic();
    final ByteArray a = new ByteArray("a");
    final ByteArray b = new ByteArray("b");
    // data written before a schema change, bypassing the DataStore validation
    statisticsStore.incorporateStatisticValue(statistic, valueOf(1, 7), a);
    statisticsStore.incorporateStatisticValue(statistic, valueOf(2, 3), a);
    statisticsStore.incorporateStatisticValue(statistic, valueOf(1, 10), b);

    assertTrue(statisticsStore.mergeStats());

    final AdditiveVectorValue merged = dataStore.getStatisticValue(statistic, BinConstraints.of(a));
    assertEquals(2, merged.getSchemaId());
    assertEquals(3L, merged.getCount(0));
    assertEquals(10L, countOf(statistic, b));
  }
}
