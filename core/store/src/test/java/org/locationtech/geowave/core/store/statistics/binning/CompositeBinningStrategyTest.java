/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.locationtech.geowave.core.store.statistics.binning.BinConstraintMatching.componentMatches;
import static org.locationtech.geowave.core.store.statistics.binning.BinConstraintMatching.readPathMatches;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.BinConstraintMatching.Kind;
import org.locationtech.geowave.core.store.statistics.binning.BinConstraintMatching.Lengths;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

public class CompositeBinningStrategyTest {
  private static final byte[] ALPHABET = {0x00, 0x7F, (byte) 0x80, (byte) 0xFF};

  @Test
  public void combinedConstraintsMatchExactlyWhatTheirChildConstraintsMatchOrAreRefused() {
    final Random random = new Random(20261102);
    final Map<String, List<Pair<ByteArray[], ByteArray>>> binsByLengths = new HashMap<>();
    final Map<String, Integer> outcomes = new TreeMap<>();
    for (int iteration = 0; iteration < 4000; iteration++) {
      final Lengths[] lengths = randomLengths(random);
      final CompositeBinningStrategy composite = composite(lengths);
      final Kind[] kinds = new Kind[lengths.length];
      final ByteArrayConstraints[] childConstraints = new ByteArrayConstraints[lengths.length];
      for (int i = 0; i < lengths.length; i++) {
        kinds[i] = BinConstraintMatching.randomKind(random);
        childConstraints[i] =
            BinConstraintMatching.randomConstraints(random, kinds[i], lengths[i], ALPHABET);
      }
      final String description =
          Arrays.toString(lengths)
              + " "
              + Arrays.toString(kinds)
              + BinConstraintMatching.describe(childConstraints);
      final String refusal = expectedRefusal(lengths, kinds, childConstraints);
      if (refusal != null) {
        assertThrows(
            description,
            IllegalArgumentException.class,
            () -> composite.constraints(childConstraints));
        outcomes.merge(refusal, 1, Integer::sum);
        continue;
      }
      final ByteArrayConstraints combined = composite.constraints(childConstraints);
      outcomes.merge(resultKind(combined), 1, Integer::sum);
      BinConstraintMatching.assertDisjoint(combined.getBinRanges());
      for (final Pair<ByteArray[], ByteArray> subBinsAndBin : binsByLengths.computeIfAbsent(
          Arrays.toString(lengths),
          k -> allBins(composite, lengths))) {
        final ByteArray[] subBins = subBinsAndBin.getLeft();
        boolean expected = true;
        for (int i = 0; i < subBins.length; i++) {
          expected &= componentMatches(childConstraints[i], subBins[i]);
        }
        assertEquals(
            description + " on " + Arrays.toString(subBins),
            expected,
            readPathMatches(combined, subBinsAndBin.getRight()));
      }
    }
    for (final String outcome : new String[] {
        "all",
        "exact",
        "prefix",
        "ranges",
        "empty",
        "illegal order",
        "not fixed length",
        "does not fit"}) {
      assertTrue(outcome + " never produced: " + outcomes, outcomes.containsKey(outcome));
    }
  }

  @Test
  public void exactTimesRangesBecomesRangesUnderEachExactBin() {
    final ByteArray hour0 = longBin(0);
    final ByteArray hour1 = longBin(3_600_000);
    final ByteArrayConstraints combined =
        timeCell().constraints(
            new Object[] {
                new ByteArray[] {hour0, hour1},
                new ByteArrayRange[] {
                    new ByteArrayRange(longBytes(10), longBytes(20)),
                    new ByteArrayRange(longBytes(30), null)}});
    assertFalse(combined.isAllBins());
    assertEquals(0, combined.getBins().length);
    assertArrayEquals(
        new ByteArrayRange[] {
            new ByteArrayRange(concat(hour0, longBin(10)), concat(hour0, longBin(20))),
            new ByteArrayRange(concat(hour0, longBin(30)), hour0.getBytes()),
            new ByteArrayRange(concat(hour1, longBin(10)), concat(hour1, longBin(20))),
            new ByteArrayRange(concat(hour1, longBin(30)), hour1.getBytes())},
        combined.getBinRanges());
  }

  @Test
  public void exactTimesAllBinsBecomesPrefixes() {
    final ByteArrayConstraints combined =
        new CompositeBinningStrategy(fixed(8, "a"), fixed(8, "b"), fixed(8, "c")).constraints(
            new Object[] {
                new ByteArray[] {longBin(1), longBin(2)},
                longBin(3),
                BinConstraints.allBins()});
    assertTrue(combined.isPrefix());
    assertArrayEquals(
        new ByteArray[] {
            new ByteArray(concat(longBin(1), longBin(3))),
            new ByteArray(concat(longBin(2), longBin(3)))},
        combined.getBins());
  }

  @Test
  public void exactTimesAllBinsNeedsOnlyTheExactChildrenToBeFixedLength() {
    final ByteArrayConstraints combined =
        new CompositeBinningStrategy(
            fixed(8, "time"),
            new FieldValueBinningStrategy("name")).constraints(
                new Object[] {longBin(1), BinConstraints.allBins()});
    assertArrayEquals(new ByteArray[] {longBin(1)}, combined.getBins());
    assertTrue(combined.isPrefix());
  }

  @Test
  public void allExactKeepsTheFullBinEncodingForAnyChildren() {
    final ByteArrayConstraints combined =
        variableNameTime().constraints(
            new Object[] {"Mi", new ByteArray[] {longBin(2), longBin(3)}});
    assertFalse(combined.isPrefix());
    assertArrayEquals(
        new ByteArray[] {
            CompositeBinningStrategy.getBin(new ByteArray("Mi"), longBin(2)),
            CompositeBinningStrategy.getBin(new ByteArray("Mi"), longBin(3))},
        combined.getBins());
  }

  @Test
  public void allBinsEverywhereIsAllBinsForAnyChildren() {
    assertTrue(
        variableNameTime().constraints(
            new Object[] {BinConstraints.allBins(), BinConstraints.allBins()}).isAllBins());
  }

  @Test
  public void emptyExactChildMatchesNothing() {
    for (final Object trailing : new Object[] {
        BinConstraints.allBins(),
        new ByteArrayRange(longBytes(1), longBytes(2)),
        BinConstraints.ofPrefix(new ByteArray(new byte[] {1})),
        longBin(1)}) {
      final ByteArrayConstraints combined =
          timeCell().constraints(new Object[] {new ByteArray[0], trailing});
      assertFalse(combined.isAllBins());
      assertEquals(0, combined.getBins().length);
      assertEquals(0, combined.getBinRanges().length);
    }
  }

  @Test
  public void constraintsThatCannotBeKeyRangesAreRejected() {
    final Object all = BinConstraints.allBins();
    final Object exact = longBin(1);
    final Object prefix = BinConstraints.ofPrefix(longBin(1));
    final Object range = new ByteArrayRange(longBytes(1), longBytes(2));
    for (final Object[] constraints : new Object[][] {
        {all, exact},
        {prefix, exact},
        {range, exact},
        {range, prefix},
        {all, range},
        {exact, all, exact},
        {exact, range, range}}) {
      final StatisticBinningStrategy[] children = new StatisticBinningStrategy[constraints.length];
      Arrays.fill(children, fixed(8, "f"));
      assertThrows(
          Arrays.toString(constraints),
          IllegalArgumentException.class,
          () -> new CompositeBinningStrategy(children).constraints(constraints));
    }
  }

  @Test
  public void constraintsThatDoNotFitAFixedLengthAreRejected() {
    for (final Object[] constraints : new Object[][] {
        {new ByteArray(new byte[4]), BinConstraints.allBins()},
        {longBin(1), BinConstraints.ofPrefix(new ByteArray(new byte[9]))},
        {longBin(1), new ByteArrayRange(longBytes(1), new byte[9])},
        {
            longBin(1),
            new ExplicitConstraints(
                new ByteArray[] {new ByteArray(new byte[4])},
                new ByteArrayRange[] {new ByteArrayRange(longBytes(1), longBytes(2))},
                false,
                false)}}) {
      assertThrows(
          Arrays.toString(constraints),
          IllegalArgumentException.class,
          () -> timeCell().constraints(constraints));
    }
  }

  /**
   * Nothing separates a variable-length child bin from the next child's bytes, so the range A..Mike
   * on names would drop "M" and "Mi" (their keys continue with the time bytes, above "Mikf"), and
   * Mike..Z would add them for some times.
   */
  @Test
  public void variableLengthChildrenCannotLeadOrTakePrefixesAndRanges() {
    for (final Object[] constraints : new Object[][] {
        {new ByteArrayRange(bytes("A"), bytes("Mike")), BinConstraints.allBins()},
        {new ByteArrayRange(bytes("Mike"), bytes("Z")), BinConstraints.allBins()},
        {BinConstraints.ofPrefix(new ByteArray("Jo")), BinConstraints.allBins()},
        {"Mike", BinConstraints.allBins()},
        {"Mike", new ByteArrayRange(longBytes(1), longBytes(2))}}) {
      assertThrows(
          Arrays.toString(constraints),
          IllegalArgumentException.class,
          () -> variableNameTime().constraints(constraints));
    }
    // the composite's bin lengths follow its last child, so the last child needs a fixed length too
    final CompositeBinningStrategy timeName =
        new CompositeBinningStrategy(fixed(8, "time"), new FieldValueBinningStrategy("name"));
    for (final Object trailing : new Object[] {
        new ByteArrayRange(bytes("A"), bytes("Mike")),
        BinConstraints.ofPrefix(new ByteArray("Jo"))}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> timeName.constraints(new Object[] {longBin(1), trailing}));
    }
  }

  @Test
  public void dataTypeTimesPartitionCannotBeQueriedByTypeAlone() {
    final CompositeBinningStrategy typePartition =
        new CompositeBinningStrategy(new DataTypeBinningStrategy(), new PartitionBinningStrategy());
    // the prefix "track" would also match the partitions of "track_points"
    assertThrows(
        IllegalArgumentException.class,
        () -> typePartition.constraints(new Object[] {"track", BinConstraints.allBins()}));
    assertArrayEquals(
        new ByteArray[] {
            CompositeBinningStrategy.getBin(
                DataTypeBinningStrategy.getBin("track"),
                PartitionBinningStrategy.getBin(new byte[] {1}))},
        typePartition.constraints(
            new Object[] {"track", PartitionBinningStrategy.getBin(new byte[] {1})}).getBins());
  }

  @Test
  public void exactBinsAlongsideRangesStayExactAndOverlapsMerge() {
    final CompositeBinningStrategy composite =
        new CompositeBinningStrategy(fixed(1, "first"), fixed(2, "last"));
    final ByteArrayConstraints combined =
        composite.constraints(
            new Object[] {
                new ByteArray("K"),
                new ExplicitConstraints(
                    new ByteArray[] {new ByteArray("ab"), new ByteArray("xa")},
                    new ByteArrayRange[] {new ByteArrayRange(bytes("x"), bytes("y"))},
                    false,
                    false)});
    // "xa" lies inside x..y, so the two become one range
    assertArrayEquals(
        new ByteArrayRange[] {
            new ByteArrayRange(bytes("Kab"), bytes("Kab")),
            new ByteArrayRange(bytes("Kx"), bytes("Ky"))},
        combined.getBinRanges());
    for (final String last : new String[] {"ab", "xa", "xz", "yb"}) {
      assertTrue(last, readPathMatches(combined, bin("K", last)));
    }
    for (final String last : new String[] {"aa", "ac", "zz"}) {
      assertFalse(last, readPathMatches(combined, bin("K", last)));
    }
  }

  @Test
  public void nestedCompositeConstraintsCombineAsAPrefix() {
    final CompositeBinningStrategy inner =
        new CompositeBinningStrategy(fixed(8, "b"), fixed(8, "c"));
    final CompositeBinningStrategy outer = new CompositeBinningStrategy(fixed(8, "a"), inner);
    final ByteArrayConstraints combined =
        outer.constraints(
            new Object[] {longBin(1), new Object[] {longBin(2), BinConstraints.allBins()}});
    assertTrue(combined.isPrefix());
    assertTrue(
        readPathMatches(
            combined,
            CompositeBinningStrategy.getBin(
                longBin(1),
                CompositeBinningStrategy.getBin(longBin(2), longBin(9)))));
    assertFalse(
        readPathMatches(
            combined,
            CompositeBinningStrategy.getBin(
                longBin(1),
                CompositeBinningStrategy.getBin(longBin(3), longBin(9)))));
  }

  @Test
  public void fixedBinLengthIncludesTheLengthSuffix() {
    assertEquals(OptionalInt.of(18), timeCell().getFixedBinLength());
    assertEquals(
        timeCell().getFixedBinLength().getAsInt(),
        CompositeBinningStrategy.getBin(longBin(1), longBin(2)).getBytes().length);
    // a length of 200 takes a two-byte varint
    assertEquals(
        OptionalInt.of(200 + 2 + 8 + 1),
        new CompositeBinningStrategy(fixed(200, "a"), fixed(8, "b")).getFixedBinLength());
    assertEquals(
        OptionalInt.of(18 + 1 + 8 + 1),
        new CompositeBinningStrategy(timeCell(), fixed(8, "c")).getFixedBinLength());
    assertFalse(variableNameTime().getFixedBinLength().isPresent());
  }

  @Test
  public void binMatchesFindsEachChildOfTwoAndThreeChildComposites() {
    final ByteArray type = DataTypeBinningStrategy.getBin("type");
    // longer than 127 bytes so its length takes a two-byte varint
    final ByteArray partition = PartitionBinningStrategy.getBin(new byte[200]);
    final ByteArray field = new ByteArray("v");

    final CompositeBinningStrategy two =
        new CompositeBinningStrategy(new DataTypeBinningStrategy(), new PartitionBinningStrategy());
    final ByteArray twoBin = CompositeBinningStrategy.getBin(type, partition);
    assertTrue(two.binMatches(DataTypeBinningStrategy.class, twoBin, type));
    assertTrue(two.binMatches(PartitionBinningStrategy.class, twoBin, partition));
    assertFalse(two.binMatches(PartitionBinningStrategy.class, twoBin, type));

    final CompositeBinningStrategy three =
        new CompositeBinningStrategy(
            new DataTypeBinningStrategy(),
            new FieldValueBinningStrategy("f"),
            new PartitionBinningStrategy());
    final ByteArray threeBin = CompositeBinningStrategy.getBin(type, field, partition);
    assertTrue(three.binMatches(DataTypeBinningStrategy.class, threeBin, type));
    assertTrue(three.binMatches(FieldValueBinningStrategy.class, threeBin, field));
    assertTrue(three.binMatches(PartitionBinningStrategy.class, threeBin, partition));
    assertFalse(three.binMatches(FieldValueBinningStrategy.class, threeBin, type));
    assertFalse(three.binMatches(PartitionBinningStrategy.class, threeBin, field));
  }

  @Test
  public void binMatchesLooksInsideNestedComposites() {
    final ByteArray type = DataTypeBinningStrategy.getBin("type");
    final ByteArray field = new ByteArray("value");
    final ByteArray partition = PartitionBinningStrategy.getBin(new byte[] {1, 2, 3});
    final CompositeBinningStrategy nested =
        new CompositeBinningStrategy(
            new DataTypeBinningStrategy(),
            new CompositeBinningStrategy(
                new FieldValueBinningStrategy("f"),
                new PartitionBinningStrategy()));
    final ByteArray bin =
        CompositeBinningStrategy.getBin(type, CompositeBinningStrategy.getBin(field, partition));
    assertTrue(nested.binMatches(PartitionBinningStrategy.class, bin, partition));
    assertTrue(nested.binMatches(FieldValueBinningStrategy.class, bin, field));
    assertFalse(nested.binMatches(PartitionBinningStrategy.class, bin, field));
  }

  @Test
  public void dataStoreReadsExactTimesRangesAndExactTimesAllBins() {
    final String typeName = "Motion";
    final MemoryRequiredOptions options = new MemoryRequiredOptions();
    options.setGeoWaveNamespace("composite-binning-strategy-test");
    final DataStore dataStore = DataStoreFactory.createDataStore(options);
    try {
      dataStore.addType(
          BasicDataTypeAdapter.newAdapter(typeName, Motion.class, "id"),
          new NullIndex("index"));
      final CountStatistic count = new CountStatistic(typeName);
      count.setTag("hour-cell");
      count.setBinningStrategy(timeCell());
      final CountStatistic variableCount = new CountStatistic(typeName);
      variableCount.setTag("variable");
      variableCount.setBinningStrategy(
          new CompositeBinningStrategy(
              new FieldValueBinningStrategy("time"),
              new FieldValueBinningStrategy("cell")));
      dataStore.addEmptyStatistic(count, variableCount);

      final long[] hours = {0, 3_600_000, 7_200_000};
      final long[] cells = {100, 101, 150, 151, 200, 201, 300};
      final Map<List<Long>, Long> stored = new HashMap<>();
      for (int h = 0; h < hours.length; h++) {
        for (int c = 0; c < cells.length; c++) {
          final long value = (h * 100) + c + 1;
          stored.put(List.of(hours[h], cells[c]), value);
          dataStore.incorporateStatisticValue(
              count,
              countValue(value),
              CompositeBinningStrategy.getBin(longBin(hours[h]), longBin(cells[c])));
        }
      }

      final Object[] exactTimesRanges =
          new Object[] {
              new ByteArray[] {longBin(hours[0]), longBin(hours[2])},
              new ByteArrayRange[] {
                  new ByteArrayRange(longBytes(100), longBytes(150)),
                  new ByteArrayRange(longBytes(201), null)}};
      final Map<List<Long>, Long> expectedExactTimesRanges =
          expected(
              stored,
              k -> ((k.get(0) == hours[0]) || (k.get(0) == hours[2]))
                  && (((k.get(1) >= 100) && (k.get(1) <= 150)) || (k.get(1) >= 201)));
      assertEquals(expectedExactTimesRanges, read(dataStore, count, exactTimesRanges));
      assertEquals(expectedExactTimesRanges, query(dataStore, count, exactTimesRanges));
      assertEquals(
          Long.valueOf(expectedExactTimesRanges.values().stream().mapToLong(Long::longValue).sum()),
          dataStore.aggregateStatistics(
              StatisticQueryBuilder.count().typeName(typeName).tag("hour-cell").binConstraints(
                  BinConstraints.ofObject(exactTimesRanges)).build()).getValue());

      final Object[] exactTimesAll =
          new Object[] {new ByteArray[] {longBin(hours[1])}, BinConstraints.allBins()};
      assertEquals(
          expected(stored, k -> k.get(0) == hours[1]),
          read(dataStore, count, exactTimesAll));

      assertThrows(
          IllegalArgumentException.class,
          () -> read(dataStore, variableCount, exactTimesAll));
    } finally {
      dataStore.deleteAll();
    }
  }

  public static class Motion {
    private String id;
    private long time;
    private long cell;

    public Motion() {}

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public long getTime() {
      return time;
    }

    public void setTime(final long time) {
      this.time = time;
    }

    public long getCell() {
      return cell;
    }

    public void setCell(final long cell) {
      this.cell = cell;
    }
  }

  private static String expectedRefusal(
      final Lengths[] lengths,
      final Kind[] kinds,
      final ByteArrayConstraints[] constraints) {
    if (!BinConstraintMatching.isLegal(kinds)) {
      return "illegal order";
    }
    int firstInexact = 0;
    while ((firstInexact < kinds.length) && (kinds[firstInexact] == Kind.EXACT)) {
      firstInexact++;
    }
    if ((firstInexact == kinds.length) || ((firstInexact == 0) && (kinds[0] == Kind.ALL))) {
      return null;
    }
    final int lastDelimited = kinds[firstInexact] == Kind.ALL ? firstInexact - 1 : firstInexact;
    for (int i = 0; i <= lastDelimited; i++) {
      if (!lengths[i].fixed) {
        return "not fixed length";
      }
      if (!BinConstraintMatching.fits(constraints[i], lengths[i].max)) {
        return "does not fit";
      }
    }
    return null;
  }

  private static Lengths[] randomLengths(final Random random) {
    final Lengths first = randomChildLengths(random, 2);
    final Lengths last = randomChildLengths(random, 2);
    return random.nextBoolean() ? new Lengths[] {first, last}
        : new Lengths[] {first, randomChildLengths(random, 1), last};
  }

  private static Lengths randomChildLengths(final Random random, final int maxLength) {
    return random.nextInt(5) < 3 ? new Lengths(1 + random.nextInt(maxLength), true)
        : new Lengths(maxLength, false);
  }

  private static CompositeBinningStrategy composite(final Lengths[] lengths) {
    final StatisticBinningStrategy[] children = new StatisticBinningStrategy[lengths.length];
    for (int i = 0; i < lengths.length; i++) {
      children[i] =
          lengths[i].fixed ? fixed(lengths[i].max, "f" + i)
              : new FieldValueBinningStrategy("f" + i);
    }
    return new CompositeBinningStrategy(children);
  }

  private static List<Pair<ByteArray[], ByteArray>> allBins(
      final CompositeBinningStrategy composite,
      final Lengths[] lengths) {
    final List<Pair<ByteArray[], ByteArray>> bins = new ArrayList<>();
    for (final ByteArray[] subBins : BinConstraintMatching.allCombinations(lengths, ALPHABET)) {
      final ByteArray bin = CompositeBinningStrategy.getBin(subBins);
      final Pair<StatisticBinningStrategy, ByteArray>[] decoded = composite.getSubBins(bin);
      for (int i = 0; i < subBins.length; i++) {
        assertEquals(subBins[i], decoded[i].getValue());
      }
      bins.add(Pair.of(subBins, bin));
    }
    return bins;
  }

  private static String resultKind(final ByteArrayConstraints constraints) {
    if (constraints.isAllBins()) {
      return "all";
    } else if (constraints.getBins().length > 0) {
      return constraints.isPrefix() ? "prefix" : "exact";
    }
    return constraints.getBinRanges().length > 0 ? "ranges" : "empty";
  }

  private static Map<List<Long>, Long> read(
      final DataStore dataStore,
      final CountStatistic count,
      final Object[] constraints) {
    final Map<List<Long>, Long> values = new HashMap<>();
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(count, BinConstraints.ofObject(constraints))) {
      while (it.hasNext()) {
        final Pair<ByteArray, Long> value = it.next();
        values.put(hourAndCell(count, value.getKey()), value.getValue());
      }
    }
    return values;
  }

  private static Map<List<Long>, Long> query(
      final DataStore dataStore,
      final CountStatistic count,
      final Object[] constraints) {
    final Map<List<Long>, Long> values = new HashMap<>();
    try (CloseableIterator<CountValue> it =
        dataStore.queryStatistics(
            StatisticQueryBuilder.count().typeName(count.getTypeName()).tag(
                count.getTag()).binConstraints(BinConstraints.ofObject(constraints)).build())) {
      while (it.hasNext()) {
        final CountValue value = it.next();
        values.put(hourAndCell(count, value.getBin()), value.getValue());
      }
    }
    return values;
  }

  private static List<Long> hourAndCell(final CountStatistic count, final ByteArray bin) {
    final Pair<StatisticBinningStrategy, ByteArray>[] subBins =
        ((CompositeBinningStrategy) count.getBinningStrategy()).getSubBins(bin);
    return List.of(
        Lexicoders.LONG.fromByteArray(subBins[0].getValue().getBytes()),
        Lexicoders.LONG.fromByteArray(subBins[1].getValue().getBytes()));
  }

  private static Map<List<Long>, Long> expected(
      final Map<List<Long>, Long> stored,
      final Predicate<List<Long>> filter) {
    return stored.entrySet().stream().filter(e -> filter.test(e.getKey())).collect(
        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static CountValue countValue(final long count) {
    final CountValue value = new CountValue();
    value.fromBinary(VarintUtils.writeSignedLong(count));
    return value;
  }

  private static FixedLengthTestBinningStrategy fixed(final int length, final String field) {
    return new FixedLengthTestBinningStrategy(length, field);
  }

  private static CompositeBinningStrategy timeCell() {
    return new CompositeBinningStrategy(fixed(8, "time"), fixed(8, "cell"));
  }

  private static CompositeBinningStrategy variableNameTime() {
    return new CompositeBinningStrategy(
        new FieldValueBinningStrategy("name"),
        new FieldValueBinningStrategy("time"));
  }

  private static ByteArray bin(final String first, final String last) {
    return CompositeBinningStrategy.getBin(new ByteArray(first), new ByteArray(last));
  }

  private static byte[] bytes(final String value) {
    return new ByteArray(value).getBytes();
  }

  private static ByteArray longBin(final long value) {
    return new ByteArray(longBytes(value));
  }

  private static byte[] longBytes(final long value) {
    return Lexicoders.LONG.toByteArray(value);
  }

  private static byte[] concat(final ByteArray first, final ByteArray second) {
    return ByteArrayUtils.combineArrays(first.getBytes(), second.getBytes());
  }
}
