/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.H3CellRanges;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.threeten.extra.Interval;
import com.beust.jcommander.JCommander;

public class EpochIntervalBinningStrategyTest {
  private static final long HOUR = 3_600_000L;
  private static final long DAY = 24 * HOUR;
  private static final long WEEK = 7 * DAY;
  // 2024-01-01T00:00:00Z is a Monday
  private static final long MONDAY_2024 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();

  private final EpochIntervalBinningStrategy hour =
      new EpochIntervalBinningStrategy(EpochInterval.HOUR, "time");
  private final EpochIntervalBinningStrategy day =
      new EpochIntervalBinningStrategy(EpochInterval.DAY, "time");
  private final EpochIntervalBinningStrategy week =
      new EpochIntervalBinningStrategy(EpochInterval.WEEK, "time");

  private static Interval interval(final long startMillis, final long endMillis) {
    return Interval.of(Instant.ofEpochMilli(startMillis), Instant.ofEpochMilli(endMillis));
  }

  @Test
  public void hourAndDayAlignToTheEpoch() {
    final long t = MONDAY_2024 + (5 * DAY) + (13 * HOUR) + 12_345L;
    assertEquals(MONDAY_2024 + (5 * DAY) + (13 * HOUR), hour.bucketStart(t));
    assertEquals(MONDAY_2024 + (5 * DAY), day.bucketStart(t));
    assertEquals(0L, hour.bucketStart(HOUR - 1));
    assertEquals(HOUR, hour.bucketStart(HOUR));
    assertEquals(0L, day.bucketStart(DAY - 1));
    assertEquals(DAY, day.bucketStart(DAY));
  }

  @Test
  public void weekStartsOnMondayUtc() {
    for (final String monday : new String[] {
        "1970-01-05T00:00:00Z",
        "2000-01-03T00:00:00Z",
        "2024-01-01T00:00:00Z",
        "2026-10-26T00:00:00Z"}) {
      final long start = Instant.parse(monday).toEpochMilli();
      assertEquals(monday, start, week.bucketStart(start));
      assertEquals(monday, start, week.bucketStart(start + WEEK - 1));
      assertEquals(monday, start + WEEK, week.bucketStart(start + WEEK));
      assertEquals(
          "the Sunday before belongs to the previous week",
          start - WEEK,
          week.bucketStart(start - 1));
    }
    // 2024-01-07T23:59:59.999Z is a Sunday
    assertEquals(
        MONDAY_2024,
        week.bucketStart(Instant.parse("2024-01-07T23:59:59.999Z").toEpochMilli()));
    // the epoch itself is a Thursday
    assertEquals(-3 * DAY, week.bucketStart(0L));
  }

  @Test
  public void timesBeforeTheEpochUseFloorDivision() {
    assertEquals(-HOUR, hour.bucketStart(-1L));
    assertEquals(-DAY, day.bucketStart(-1L));
    assertEquals(-DAY, day.bucketStart(-DAY));
    assertEquals(-2 * DAY, day.bucketStart(-DAY - 1));
    // 1969-12-28 is a Sunday; its week starts Monday 1969-12-22
    final long sunday = Instant.parse("1969-12-28T12:00:00Z").toEpochMilli();
    assertEquals(Instant.parse("1969-12-22T00:00:00Z").toEpochMilli(), week.bucketStart(sunday));
    assertEquals(
        Instant.parse("1969-12-29T00:00:00Z").toEpochMilli(),
        week.bucketStart(Instant.parse("1969-12-29T00:00:00Z").toEpochMilli()));
  }

  @Test
  public void binsAreEightBytesAndSortInTimeOrder() {
    final long[] times =
        {
            Instant.parse("1960-06-01T00:00:00Z").toEpochMilli(),
            -1L,
            0L,
            MONDAY_2024,
            MONDAY_2024 + DAY,
            Instant.parse("2100-01-01T00:00:00Z").toEpochMilli()};
    ByteArray previous = null;
    for (final long t : times) {
      final ByteArray bin = day.getBin(t);
      assertEquals(Long.BYTES, bin.getBytes().length);
      if (previous != null) {
        assertTrue(previous.compareTo(bin) < 0);
      }
      previous = bin;
    }
  }

  @Test
  public void intervalOfBinContainsTime() {
    for (final EpochIntervalBinningStrategy s : Arrays.asList(hour, day, week)) {
      for (final long t : new long[] {-123456789L, 0L, MONDAY_2024 + 987654321L}) {
        final Interval i = s.getInterval(s.getBin(t));
        assertTrue(i.contains(Instant.ofEpochMilli(t)));
        assertEquals(s.getInterval().getWidthMillis(), i.toDuration().toMillis());
        assertEquals(s.bucketStart(t), i.getStart().toEpochMilli());
      }
    }
  }

  @Test
  public void binRangesCoverExactlyTheOverlappingBuckets() {
    final ByteArrayRange[] ranges =
        day.getBinRanges(interval(MONDAY_2024 + (2 * HOUR), MONDAY_2024 + (2 * DAY) + HOUR));
    assertEquals(1, ranges.length);
    assertArrayEquals(day.getBin(MONDAY_2024).getBytes(), ranges[0].getStart());
    assertArrayEquals(day.getBin(MONDAY_2024 + (2 * DAY)).getBytes(), ranges[0].getEnd());

    final ByteArrayRange[] aligned = day.getBinRanges(interval(MONDAY_2024, MONDAY_2024 + DAY));
    assertEquals(1, aligned.length);
    assertArrayEquals(day.getBin(MONDAY_2024).getBytes(), aligned[0].getStart());
    assertArrayEquals(day.getBin(MONDAY_2024).getBytes(), aligned[0].getEnd());
  }

  @Test
  public void emptyIntervalHasNoBinRangeSoRangeDeletesDeleteNothing() {
    assertEquals(0, day.getBinRanges(interval(MONDAY_2024 + HOUR, MONDAY_2024 + HOUR)).length);
    assertEquals(0, hour.getBinRanges(interval(MONDAY_2024, MONDAY_2024)).length);
  }

  @Test
  public void unboundedIntervalsAreOpenBinRanges() {
    final ByteArrayRange[] endingAt =
        hour.getBinRanges(Interval.endingAt(Instant.ofEpochMilli(MONDAY_2024 + (5 * HOUR))));
    assertEquals(1, endingAt.length);
    assertNull(endingAt[0].getStart());
    assertArrayEquals(hour.getBin(MONDAY_2024 + (4 * HOUR)).getBytes(), endingAt[0].getEnd());

    final ByteArrayRange[] startingAt =
        hour.getBinRanges(Interval.startingAt(Instant.ofEpochMilli(MONDAY_2024 + (5 * HOUR))));
    assertEquals(1, startingAt.length);
    assertArrayEquals(hour.getBin(MONDAY_2024 + (5 * HOUR)).getBytes(), startingAt[0].getStart());
    assertNull(startingAt[0].getEnd());

    final ByteArrayRange[] all = hour.getBinRanges(Interval.ALL);
    assertEquals(1, all.length);
    assertNull(all[0].getStart());
    assertNull(all[0].getEnd());
  }

  @Test
  public void unboundedIntervalConstraintsAreOpenRanges() {
    final Instant cutoff = Instant.ofEpochMilli(MONDAY_2024 + (5 * HOUR));
    ByteArrayConstraints c = hour.constraints(Interval.endingAt(cutoff));
    assertEquals(0, c.getBins().length);
    assertEquals(1, c.getBinRanges().length);
    assertNull(c.getBinRanges()[0].getStart());
    assertArrayEquals(
        hour.getBin(MONDAY_2024 + (4 * HOUR)).getBytes(),
        c.getBinRanges()[0].getEnd());

    c = hour.constraints(Interval.startingAt(cutoff));
    assertEquals(1, c.getBinRanges().length);
    assertArrayEquals(
        hour.getBin(cutoff.toEpochMilli()).getBytes(),
        c.getBinRanges()[0].getStart());
    assertNull(c.getBinRanges()[0].getEnd());

    c = hour.constraints(Interval.ALL);
    assertEquals(1, c.getBinRanges().length);
    assertNull(c.getBinRanges()[0].getStart());
    assertNull(c.getBinRanges()[0].getEnd());

    c = hour.constraints(new Interval[] {Interval.endingAt(cutoff), interval(0, HOUR)});
    assertEquals(0, c.getBins().length);
    assertEquals(
        "a window inside the open range merges into it",
        Arrays.asList(endingAt(cutoff)),
        Arrays.asList(c.getBinRanges()));

    c =
        hour.constraints(
            new Interval[] {
                Interval.startingAt(cutoff),
                interval(MONDAY_2024, MONDAY_2024 + HOUR)});
    assertEquals(
        Arrays.asList(
            new ByteArrayRange(
                hour.getBin(MONDAY_2024).getBytes(),
                hour.getBin(MONDAY_2024).getBytes()),
            new ByteArrayRange(hour.getBin(cutoff.toEpochMilli()).getBytes(), null)),
        Arrays.asList(c.getBinRanges()));
  }

  private ByteArrayRange endingAt(final Instant cutoff) {
    return new ByteArrayRange(null, hour.getBin(cutoff.toEpochMilli() - 1).getBytes());
  }

  private static void assertOneRange(
      final ByteArrayRange expected,
      final ByteArrayConstraints constraints) {
    assertEquals(0, constraints.getBins().length);
    assertEquals(Arrays.asList(expected), Arrays.asList(constraints.getBinRanges()));
  }

  @Test
  public void farBoundedWindowsAreOneRangeLikeTheirBinRange() {
    final Instant cutoff = Instant.parse("2026-10-26T05:00:00Z");
    for (final Interval window : new Interval[] {
        Interval.of(Instant.ofEpochMilli(Long.MIN_VALUE / 2), cutoff),
        Interval.of(cutoff, Instant.ofEpochMilli(Long.MAX_VALUE / 2))}) {
      final ByteArrayRange[] binRanges = hour.getBinRanges(window);
      assertEquals(1, binRanges.length);
      assertNotNull(binRanges[0].getStart());
      assertNotNull(binRanges[0].getEnd());
      assertOneRange(binRanges[0], hour.constraints(window));
      assertOneRange(binRanges[0], hour.constraints(new Interval[] {window}));
    }
  }

  @Test
  public void boundedWindowsPastTheBucketLimitAreOneRange() {
    final Instant cutoff = Instant.parse("2026-10-26T05:00:00Z");
    final Instant start = Instant.parse("1900-01-01T00:00:00Z");
    assertOneRange(
        new ByteArrayRange(
            hour.getBin(start.toEpochMilli()).getBytes(),
            hour.getBin(cutoff.toEpochMilli() - 1).getBytes()),
        hour.constraints(Interval.of(start, cutoff)));

    final long limit = EpochIntervalBinningStrategy.MAX_EXACT_BUCKETS;
    final ByteArrayConstraints atLimit =
        hour.constraints(interval(MONDAY_2024, MONDAY_2024 + (limit * HOUR)));
    assertEquals(limit, atLimit.getBins().length);
    assertEquals(0, atLimit.getBinRanges().length);
    assertEquals(hour.getBin(MONDAY_2024), atLimit.getBins()[0]);
    assertEquals(
        hour.getBin(MONDAY_2024 + ((limit - 1) * HOUR)),
        atLimit.getBins()[(int) limit - 1]);

    assertOneRange(
        new ByteArrayRange(
            hour.getBin(MONDAY_2024).getBytes(),
            hour.getBin(MONDAY_2024 + (limit * HOUR)).getBytes()),
        hour.constraints(interval(MONDAY_2024, MONDAY_2024 + ((limit + 1) * HOUR))));
  }

  @Test
  public void theBucketLimitCountsEveryWindowOfAnArrayOnce() {
    final long half = (EpochIntervalBinningStrategy.MAX_EXACT_BUCKETS / 2) + 1;
    final Interval first = interval(MONDAY_2024, MONDAY_2024 + (half * HOUR));
    final Interval overlapping = interval(MONDAY_2024 + HOUR, MONDAY_2024 + (half * HOUR));
    assertEquals(half, hour.constraints(new Interval[] {first, overlapping}).getBins().length);

    final Interval later =
        interval(MONDAY_2024 + ((half + 1) * HOUR), MONDAY_2024 + (((2 * half) + 1) * HOUR));
    final ByteArrayConstraints c = hour.constraints(new Interval[] {later, first});
    assertEquals(0, c.getBins().length);
    assertEquals(
        Arrays.asList(hour.getBinRanges(first)[0], hour.getBinRanges(later)[0]),
        Arrays.asList(c.getBinRanges()));
  }

  @Test
  public void timesPastTheMillisecondLimitsClampToTheOutermostBuckets() {
    final ByteArray first = hour.getBin(Long.MIN_VALUE / 2);
    final ByteArray last = hour.getBin(Long.MAX_VALUE / 2);
    assertEquals("no overflow past the first bucket", first, hour.getBin(Long.MIN_VALUE));
    assertEquals(last, hour.getBin(Long.MAX_VALUE));
    assertTrue(first.compareTo(hour.getBin(0L)) < 0);
    assertEquals(Instant.parse("-146136543-09-08T08:00:00Z"), hour.getInterval(first).getStart());
    assertEquals(Instant.parse("+146140482-04-24T15:00:00Z"), hour.getInterval(last).getStart());

    assertEquals(Arrays.asList(first), Arrays.asList(hour.constraints(Instant.MIN).getBins()));
    assertEquals(Arrays.asList(last), Arrays.asList(hour.constraints(Instant.MAX).getBins()));

    final ByteArrayRange upToFirst = new ByteArrayRange(null, first.getBytes());
    assertOneRange(upToFirst, hour.constraints(Interval.endingAt(Instant.MIN)));
    assertEquals(
        "an empty window has no bin range",
        0,
        hour.getBinRanges(Interval.endingAt(Instant.MIN)).length);
    final Interval firstSecond = Interval.of(Instant.MIN, Instant.MIN.plusSeconds(1));
    assertOneRange(upToFirst, hour.constraints(firstSecond));
    assertEquals(Arrays.asList(upToFirst), Arrays.asList(hour.getBinRanges(firstSecond)));
    assertOneRange(
        new ByteArrayRange(last.getBytes(), null),
        hour.constraints(Interval.startingAt(Instant.MAX)));
  }

  @Test
  public void aSubMillisecondEndStillSelectsTheBucketOfItsLastInstant() {
    final Instant start = Instant.ofEpochMilli(MONDAY_2024);
    final Interval window = Interval.of(start.minusSeconds(60), start.plusNanos(500_000));
    assertEquals(
        Arrays.asList(hour.getBin(MONDAY_2024 - HOUR), hour.getBin(MONDAY_2024)),
        Arrays.asList(hour.constraints(window).getBins()));
    assertEquals(
        Arrays.asList(hour.getBin(MONDAY_2024)),
        Arrays.asList(hour.constraints(Interval.of(start, start.plusNanos(1))).getBins()));
  }

  @Test
  public void aTimeLedCompositeTakesAWindowPastTheBucketLimitOnlyBeforeAllBins() {
    final CompositeBinningStrategy composite =
        new CompositeBinningStrategy(hour, new H3CellFieldValueBinningStrategy(5, "cell"));
    final long cell = H3CellRanges.h3().latLngToCell(38.9, -77.0, 5);
    final long limit = EpochIntervalBinningStrategy.MAX_EXACT_BUCKETS;
    final Interval atLimit = interval(MONDAY_2024, MONDAY_2024 + (limit * HOUR));
    assertEquals(limit, composite.constraints(new Object[] {atLimit, cell}).getBins().length);

    final Interval pastLimit = interval(MONDAY_2024, MONDAY_2024 + ((limit + 1) * HOUR));
    assertEquals(
        1,
        composite.constraints(
            new Object[] {pastLimit, BinConstraints.allBins()}).getBinRanges().length);
    final IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> composite.constraints(new Object[] {pastLimit, cell}));
    assertEquals(
        "Cannot constrain binning strategy 1 (H3_CELL) after the 'all bins', prefix or range constraint on binning strategy 0 (EPOCH_INTERVAL); only 'all bins' may follow such a constraint",
        e.getMessage());
  }

  @Test
  public void singleTimeConstraintsAreExactBins() {
    final long t = MONDAY_2024 + (3 * DAY) + (7 * HOUR) + 5;
    final ByteArray expected = day.getBin(t);
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(t);
    for (final Object constraint : new Object[] {
        new Date(t),
        calendar,
        Long.valueOf(t),
        Instant.ofEpochMilli(t)}) {
      final ByteArrayConstraints c = day.constraints(constraint);
      assertEquals(Arrays.asList(expected), Arrays.asList(c.getBins()));
      assertEquals(0, c.getBinRanges().length);
      assertTrue(!c.isPrefix() && !c.isAllBins());
    }
  }

  @Test
  public void intervalConstraintsExpandToEveryOverlappingBucket() {
    final ByteArrayConstraints c =
        hour.constraints(interval(MONDAY_2024 + (30 * 60_000L), MONDAY_2024 + (3 * HOUR) + 1));
    assertEquals(
        Arrays.asList(
            hour.getBin(MONDAY_2024),
            hour.getBin(MONDAY_2024 + HOUR),
            hour.getBin(MONDAY_2024 + (2 * HOUR)),
            hour.getBin(MONDAY_2024 + (3 * HOUR))),
        Arrays.asList(c.getBins()));
  }

  @Test
  public void intervalEndIsExclusive() {
    final ByteArrayConstraints c =
        hour.constraints(interval(MONDAY_2024, MONDAY_2024 + (2 * HOUR)));
    assertEquals(
        Arrays.asList(hour.getBin(MONDAY_2024), hour.getBin(MONDAY_2024 + HOUR)),
        Arrays.asList(c.getBins()));
  }

  @Test
  public void emptyIntervalIsTheBucketContainingItsStart() {
    final ByteArrayConstraints aligned =
        hour.constraints(interval(MONDAY_2024 + HOUR, MONDAY_2024 + HOUR));
    assertEquals(Arrays.asList(hour.getBin(MONDAY_2024 + HOUR)), Arrays.asList(aligned.getBins()));
    final ByteArrayConstraints inside =
        hour.constraints(interval(MONDAY_2024 + HOUR + 17, MONDAY_2024 + HOUR + 17));
    assertEquals(Arrays.asList(hour.getBin(MONDAY_2024 + HOUR)), Arrays.asList(inside.getBins()));
  }

  @Test
  public void intervalArraysAreSortedAndDeduplicated() {
    final ByteArrayConstraints c =
        day.constraints(
            new Interval[] {
                interval(MONDAY_2024 + (4 * DAY), MONDAY_2024 + (6 * DAY)),
                interval(MONDAY_2024, MONDAY_2024 + (2 * DAY)),
                interval(MONDAY_2024 + DAY, MONDAY_2024 + (2 * DAY) + 1)});
    assertEquals(
        Arrays.asList(
            day.getBin(MONDAY_2024),
            day.getBin(MONDAY_2024 + DAY),
            day.getBin(MONDAY_2024 + (2 * DAY)),
            day.getBin(MONDAY_2024 + (4 * DAY)),
            day.getBin(MONDAY_2024 + (5 * DAY))),
        Arrays.asList(c.getBins()));
  }

  @Test
  public void otherConstraintsFallBackToTheParent() {
    final ByteArray raw = new ByteArray(new byte[] {1, 2});
    assertEquals(Arrays.asList(raw), Arrays.asList(day.constraints(raw).getBins()));
    assertEquals(0, day.constraints(new Object()).getBins().length);
  }

  @Test
  public void binLengthIsFixedAtEightBytes() {
    assertEquals(Long.BYTES, hour.getFixedBinLength().getAsInt());
    assertEquals(Long.BYTES, hour.getBin(-1L).getBytes().length);
  }

  private static EpochIntervalBinningStrategy fromCommandLine(final String... args) {
    final EpochIntervalBinningStrategy parsed = new EpochIntervalBinningStrategy();
    JCommander.newBuilder().addObject(parsed).build().parse(args);
    return parsed;
  }

  @Test
  public void multipleFieldsAreRejectedBeforeUse() {
    final EpochIntervalBinningStrategy twoFields =
        fromCommandLine("--binField", "time", "--binField", "other");
    assertThrows(IllegalArgumentException.class, () -> twoFields.constraints(interval(0, HOUR)));
    assertThrows(IllegalArgumentException.class, twoFields::toBinary);
  }

  @Test
  public void commandLineParsesTheIntervalAndOneField() {
    final EpochIntervalBinningStrategy parsed =
        fromCommandLine("--binField", "time", "--binInterval", "HOUR");
    assertEquals(EpochInterval.HOUR, parsed.getInterval());
    assertEquals("time-HOUR", parsed.getDefaultTag());
  }

  @Test
  public void binToStringAndDefaultTag() {
    assertEquals(
        "2024-01-01T00:00:00Z/2024-01-02T00:00:00Z",
        day.binToString(day.getBin(MONDAY_2024)));
    assertEquals("time-WEEK", week.getDefaultTag());
  }

  @Test
  public void persistenceRoundTrip() {
    for (final EpochInterval i : EpochInterval.values()) {
      final EpochIntervalBinningStrategy original = new EpochIntervalBinningStrategy(i, "time");
      final EpochIntervalBinningStrategy copy =
          (EpochIntervalBinningStrategy) PersistenceUtils.fromBinary(
              PersistenceUtils.toBinary(original));
      assertNotNull(copy);
      assertEquals(i, copy.getInterval());
      assertEquals(original.getDefaultTag(), copy.getDefaultTag());
      assertEquals(original.getBin(MONDAY_2024 + 1), copy.getBin(MONDAY_2024 + 1));
    }
  }

  public static class Reading {
    private String id;
    private Date time;

    public Reading() {}

    public Reading(final String id, final Date time) {
      this.id = id;
      this.time = time;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public Date getTime() {
      return time;
    }

    public void setTime(final Date time) {
      this.time = time;
    }
  }

  private DataStore dataStore;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(BasicDataTypeAdapter.newAdapter("Reading", Reading.class, "id"));
  }

  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  @Test
  public void entriesWithoutATimeHaveNoBins() {
    final BasicDataTypeAdapter<Reading> adapter =
        BasicDataTypeAdapter.newAdapter("Reading", Reading.class, "id");
    assertEquals(0, day.getBins(adapter, new Reading("a", null)).length);
    assertEquals(1, day.getBins(adapter, new Reading("b", new Date(MONDAY_2024))).length);
  }

  @Test
  public void binnedCountRoundTripsThroughTheDataStore() {
    final CountStatistic count = new CountStatistic("Reading");
    count.setTag("by-day");
    count.setBinningStrategy(new EpochIntervalBinningStrategy(EpochInterval.DAY, "time"));
    dataStore.addEmptyStatistic(count);

    final List<Long> days = new ArrayList<>();
    for (int d = 0; d < 5; d++) {
      days.add(MONDAY_2024 + (d * DAY));
      final CountValue value = count.createEmpty();
      value.fromBinary(VarintUtils.writeSignedLong(10L * (d + 1)));
      dataStore.incorporateStatisticValue(count, value, day.getBin(MONDAY_2024 + (d * DAY)));
    }

    final Interval query = interval(MONDAY_2024 + DAY + HOUR, MONDAY_2024 + (3 * DAY));
    final List<Pair<ByteArray, Long>> results = new ArrayList<>();
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(count, BinConstraints.ofObject(query))) {
      while (it.hasNext()) {
        results.add(it.next());
      }
    }
    assertEquals(2, results.size());
    long total = 0;
    for (final Pair<ByteArray, Long> r : results) {
      total += r.getValue();
    }
    assertEquals(20L + 30L, total);
  }
}
