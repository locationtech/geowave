/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.LongStream;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import org.threeten.extra.Interval;
import com.beust.jcommander.Parameter;

/**
 * Statistic binning strategy that bins by the fixed UTC bucket (hour, day or week) containing the
 * value of a single time field. Each bin is the bucket start in epoch milliseconds, encoded as an 8
 * byte sortable long, so bins sort in time order and have a fixed length.
 */
public class EpochIntervalBinningStrategy extends FieldValueBinningStrategy {
  public static final String NAME = "EPOCH_INTERVAL";
  /**
   * The most buckets that bounded windows are constrained to as exact bins. Past it they become
   * ranges, like an unbounded window, instead of every bucket being enumerated.
   */
  public static final long MAX_EXACT_BUCKETS = 100_000;
  protected static final EpochInterval DEFAULT_INTERVAL = EpochInterval.DAY;
  // bucket arithmetic cannot overflow within these limits, and a window side past one is unbounded
  private static final long MIN_MILLIS = Long.MIN_VALUE / 2;
  private static final long MAX_MILLIS = Long.MAX_VALUE / 2;
  private static final Instant MIN_INSTANT = Instant.ofEpochMilli(MIN_MILLIS);
  private static final Instant MAX_INSTANT = Instant.ofEpochMilli(MAX_MILLIS);

  @Parameter(
      names = {"--binInterval"},
      required = false,
      description = "The fixed UTC interval at which to bin time values: HOUR, DAY or WEEK (starting Monday). Defaults to DAY.")
  protected EpochInterval interval = DEFAULT_INTERVAL;

  public EpochIntervalBinningStrategy() {
    super();
  }

  public EpochIntervalBinningStrategy(final EpochInterval interval, final String field) {
    super(field);
    this.interval = interval;
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic by the fixed UTC hour, day or week of the time value of a specified field.";
  }

  public EpochInterval getInterval() {
    return interval;
  }

  @Override
  public OptionalInt getFixedBinLength() {
    return OptionalInt.of(Long.BYTES);
  }

  private void requireSingleField() {
    if (fields.size() > 1) {
      throw new IllegalArgumentException(
          NAME
              + " bins by exactly one field, but --binField was given "
              + fields.size()
              + " times");
    }
  }

  @Override
  public String getDefaultTag() {
    return super.getDefaultTag() + "-" + interval.name();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<?>[] supportedConstraintClasses() {
    return ArrayUtils.addAll(
        super.supportedConstraintClasses(),
        Date.class,
        Calendar.class,
        Number.class,
        Instant.class,
        Interval.class,
        Interval[].class);
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    if (fields.isEmpty()) {
      return new ByteArray[0];
    }
    requireSingleField();
    final long millis = toMillis(adapter.getFieldValue(entry, fields.get(0)));
    if (millis == TimeUtils.RESERVED_MILLIS_FOR_NULL) {
      return new ByteArray[0];
    }
    return new ByteArray[] {getBin(millis)};
  }

  @Override
  public ByteArrayConstraints constraints(final Object constraint) {
    requireSingleField();
    return super.constraints(constraint);
  }

  /**
   * An Interval selects the buckets overlapping [start, end), except that an empty Interval, which
   * is how a single instant is written, selects the bucket containing it. Overlapping and adjacent
   * Intervals of an Interval[] merge. A window with an unbounded side (endingAt, startingAt, ALL),
   * or bounded windows of more than {@link #MAX_EXACT_BUCKETS} buckets in all, become ranges
   * instead of exact bins, so in a composite only 'all bins' may follow them.
   */
  @Override
  public ByteArrayConstraints singleFieldConstraints(final Object constraint) {
    if (constraint instanceof Interval) {
      return windowConstraints((Interval) constraint);
    } else if (constraint instanceof Interval[]) {
      return windowConstraints((Interval[]) constraint);
    }
    final long millis = toMillis(constraint);
    if (millis != TimeUtils.RESERVED_MILLIS_FOR_NULL) {
      return new ExplicitConstraints(new ByteArray[] {getBin(millis)});
    }
    return super.singleFieldConstraints(constraint);
  }

  private ByteArrayConstraints windowConstraints(final Interval... windows) {
    final List<long[]> spans = bucketSpans(windows);
    if (spans.stream().anyMatch(EpochIntervalBinningStrategy::isUnbounded)
        || (spans.stream().mapToLong(this::bucketCount).sum() > MAX_EXACT_BUCKETS)) {
      return new ExplicitConstraints(
          spans.stream().map(EpochIntervalBinningStrategy::toRange).toArray(ByteArrayRange[]::new));
    }
    final long width = interval.getWidthMillis();
    return new ExplicitConstraints(
        spans.stream().flatMapToLong(
            s -> LongStream.range(0, bucketCount(s)).map(i -> s[0] + (i * width))).mapToObj(
                EpochIntervalBinningStrategy::toBin).toArray(ByteArray[]::new));
  }

  // by bucket index, since the millis between the first and last bucket can overflow
  private long bucketCount(final long[] span) {
    final long width = interval.getWidthMillis();
    return (Math.floorDiv(span[1], width) - Math.floorDiv(span[0], width)) + 1;
  }

  private static long toMillis(final Object value) {
    if (value instanceof Instant) {
      final Instant instant = (Instant) value;
      return instant.isBefore(MIN_INSTANT) ? MIN_MILLIS
          : instant.isAfter(MAX_INSTANT) ? MAX_MILLIS : instant.toEpochMilli();
    }
    if ((value instanceof Date) || (value instanceof Calendar) || (value instanceof Number)) {
      return TimeUtils.getTimeMillis(value);
    }
    return TimeUtils.RESERVED_MILLIS_FOR_NULL;
  }

  /**
   * The start of the bucket containing a time. Times more than about 146 million years from the
   * epoch fall in the bucket at that limit.
   */
  public long bucketStart(final long millis) {
    return interval.bucketStart(Math.max(MIN_MILLIS, Math.min(MAX_MILLIS, millis)));
  }

  public ByteArray getBin(final long millis) {
    return toBin(bucketStart(millis));
  }

  private static ByteArray toBin(final long bucketStart) {
    return new ByteArray(Lexicoders.LONG.toByteArray(bucketStart));
  }

  public Interval getInterval(final ByteArray bin) {
    final long start = Lexicoders.LONG.fromByteArray(bin.getBytes());
    return Interval.of(
        Instant.ofEpochMilli(start),
        Instant.ofEpochMilli(start + interval.getWidthMillis()));
  }

  /**
   * The contiguous bin range of the buckets overlapping [start, end), for range queries and range
   * deletes. An empty Interval contains no instant, so it gets no range and deletes nothing; this
   * differs from constraints(), where an empty Interval selects the bucket containing its instant.
   * An unbounded end (endingAt, startingAt, ALL) is a null range end.
   */
  public ByteArrayRange[] getBinRanges(final Interval window) {
    return window.isEmpty() ? new ByteArrayRange[0]
        : new ByteArrayRange[] {toRange(bucketSpan(window))};
  }

  // the first and last bucket starts of each window, sorted and with overlapping and adjacent
  // windows merged; an unbounded side is Long.MIN_VALUE or Long.MAX_VALUE
  private List<long[]> bucketSpans(final Interval[] windows) {
    final long width = interval.getWidthMillis();
    final List<long[]> merged = new ArrayList<>();
    Arrays.stream(windows).map(this::bucketSpan).sorted(
        Comparator.comparingLong(s -> s[0])).forEachOrdered(span -> {
          final long[] last = merged.isEmpty() ? null : merged.get(merged.size() - 1);
          if ((last != null) && ((last[1] == Long.MAX_VALUE) || (span[0] <= (last[1] + width)))) {
            last[1] = Math.max(last[1], span[1]);
          } else {
            merged.add(span);
          }
        });
    return merged;
  }

  private long[] bucketSpan(final Interval window) {
    return new long[] {
        window.getStart().isBefore(MIN_INSTANT) ? Long.MIN_VALUE
            : bucketStart(toMillis(window.getStart())),
        window.getEnd().isAfter(MAX_INSTANT) ? Long.MAX_VALUE
            : bucketStart(toMillis(lastInstant(window)))};
  }

  // the end is exclusive, except for an empty Interval which selects the bucket of its start
  private static Instant lastInstant(final Interval window) {
    return window.isEmpty() ? window.getStart() : window.getEnd().minusNanos(1);
  }

  private static boolean isUnbounded(final long[] span) {
    return (span[0] == Long.MIN_VALUE) || (span[1] == Long.MAX_VALUE);
  }

  private static ByteArrayRange toRange(final long[] span) {
    return new ByteArrayRange(
        span[0] == Long.MIN_VALUE ? null : toBin(span[0]).getBytes(),
        span[1] == Long.MAX_VALUE ? null : toBin(span[1]).getBytes());
  }

  @Override
  public String binToString(final ByteArray bin) {
    return getInterval(bin).toString();
  }

  @Override
  public byte[] toBinary() {
    requireSingleField();
    final byte[] parentBinary = super.toBinary();
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(interval.ordinal()) + parentBinary.length);
    VarintUtils.writeUnsignedInt(interval.ordinal(), buf);
    buf.put(parentBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    interval = EpochInterval.values()[VarintUtils.readUnsignedInt(buf)];
    final byte[] parentBinary = new byte[buf.remaining()];
    buf.get(parentBinary);
    super.fromBinary(parentBinary);
    requireSingleField();
  }
}
