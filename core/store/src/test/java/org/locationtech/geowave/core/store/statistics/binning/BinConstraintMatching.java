/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.IntSupplier;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

/**
 * Evaluates bin constraints against bins, either on a whole bin the way the statistics read path
 * does or on one component of a bin, and generates random component constraints.
 */
final class BinConstraintMatching {
  // stands in for the statistic id that StatisticValue.getValueId puts in front of every bin
  private static final byte[] VALUE_ID_PREFIX = {0x10, 0x20, 0x00};

  enum Kind {
    EXACT, ALL, PREFIX, RANGES
  }

  /** The bins of one component: all of one length if fixed, otherwise of any length up to max. */
  static final class Lengths {
    final int max;
    final boolean fixed;

    Lengths(final int max, final boolean fixed) {
      this.max = max;
      this.fixed = fixed;
    }

    @Override
    public String toString() {
      return (fixed ? "fixed " : "up to ") + max;
    }
  }

  private BinConstraintMatching() {}

  /**
   * Exact bins are point gets, prefix bins are prefix scans, and ranges (used only without bins)
   * scan from the start to the end's next prefix.
   */
  static boolean readPathMatches(final ByteArrayConstraints constraints, final ByteArray bin) {
    if (constraints.isAllBins()) {
      return true;
    }
    final byte[] key = valueId(bin.getBytes());
    if (constraints.getBins().length > 0) {
      return Arrays.stream(constraints.getBins()).map(b -> valueId(b.getBytes())).anyMatch(
          b -> constraints.isPrefix() ? ByteArrayUtils.startsWith(key, b) : Arrays.equals(key, b));
    }
    return Arrays.stream(constraints.getBinRanges()).anyMatch(r -> {
      // a null end would be stored unbounded, escaping the statistic's own values
      assertNotNull("range start", r.getStart());
      assertNotNull("range end", r.getEnd());
      final ByteArrayRange keyRange =
          new ByteArrayRange(valueId(r.getStart()), valueId(r.getEnd()));
      return (ByteArrayUtils.compare(key, keyRange.getStart()) >= 0)
          && (ByteArrayUtils.compare(key, keyRange.getEndAsNextPrefix()) < 0);
    });
  }

  /**
   * Exact bins match equal bins and prefixes match the bins that start with them, ranges match from
   * the start to the end's next prefix with null ends unbounded, and bins with ranges are a union.
   */
  static boolean componentMatches(final ByteArrayConstraints constraints, final ByteArray subBin) {
    if (constraints.isAllBins()) {
      return true;
    }
    final byte[] bytes = subBin.getBytes();
    return Arrays.stream(constraints.getBins()).anyMatch(
        b -> constraints.isPrefix() ? ByteArrayUtils.startsWith(bytes, b.getBytes())
            : Arrays.equals(bytes, b.getBytes()))
        || Arrays.stream(constraints.getBinRanges()).anyMatch(
            r -> ((r.getStart() == null) || (ByteArrayUtils.compare(bytes, r.getStart()) >= 0))
                && ((r.getEnd() == null)
                    || (ByteArrayUtils.compare(bytes, r.getEndAsNextPrefix()) < 0)));
  }

  static void assertDisjoint(final ByteArrayRange[] ranges) {
    for (int i = 0; i < ranges.length; i++) {
      for (int j = i + 1; j < ranges.length; j++) {
        assertFalse(
            "ranges overlap: " + describe(ranges[i]) + " and " + describe(ranges[j]),
            ranges[i].intersects(ranges[j]));
      }
    }
  }

  static boolean isLegal(final Kind[] kinds) {
    boolean afterInexact = false;
    for (final Kind kind : kinds) {
      if (afterInexact && (kind != Kind.ALL)) {
        return false;
      }
      afterInexact |= kind != Kind.EXACT;
    }
    return true;
  }

  /** Whether constraints on a component of a fixed length fit that length. */
  static boolean fits(final ByteArrayConstraints constraints, final int length) {
    return Arrays.stream(constraints.getBins()).allMatch(
        b -> constraints.isPrefix() ? b.getBytes().length <= length : b.getBytes().length == length)
        && Arrays.stream(constraints.getBinRanges()).allMatch(
            r -> ((r.getStart() == null) || (r.getStart().length <= length))
                && ((r.getEnd() == null) || (r.getEnd().length <= length)));
  }

  static Kind randomKind(final Random random) {
    final int r = random.nextInt(8);
    return r < 3 ? Kind.EXACT : r < 5 ? Kind.ALL : r < 6 ? Kind.PREFIX : Kind.RANGES;
  }

  /** Constraints on a fixed-length component sometimes do not fit it. */
  static ByteArrayConstraints randomConstraints(
      final Random random,
      final Kind kind,
      final Lengths lengths,
      final byte[] alphabet) {
    final IntSupplier exactLength =
        () -> lengths.fixed ? (random.nextInt(20) == 0 ? lengths.max + 1 : lengths.max)
            : random.nextInt(lengths.max + 1);
    final IntSupplier partialLength =
        () -> lengths.fixed && (random.nextInt(20) == 0) ? lengths.max + 1
            : random.nextInt(lengths.max + 1);
    switch (kind) {
      case ALL:
        return new ExplicitConstraints(true);
      case EXACT:
        return new ExplicitConstraints(
            randomBins(
                random,
                random.nextInt(10) == 0 ? 0 : 1 + random.nextInt(3),
                alphabet,
                exactLength));
      case PREFIX:
        return new ExplicitConstraints(
            randomBins(random, 1 + random.nextInt(3), alphabet, partialLength),
            true);
      case RANGES:
      default:
        final List<ByteArrayRange> ranges = new ArrayList<>();
        for (int i = 1 + random.nextInt(3); i > 0; i--) {
          ranges.add(
              new ByteArrayRange(
                  randomRangeBound(random, alphabet, partialLength),
                  randomRangeBound(random, alphabet, partialLength)));
        }
        final ByteArray[] bins =
            random.nextInt(4) == 0
                ? randomBins(random, 1 + random.nextInt(2), alphabet, exactLength)
                : new ByteArray[0];
        return new ExplicitConstraints(bins, ranges.toArray(new ByteArrayRange[0]), false, false);
    }
  }

  /** Every combination of one value of each component. */
  static List<ByteArray[]> allCombinations(final Lengths[] lengths, final byte[] alphabet) {
    List<ByteArray[]> combinations = List.<ByteArray[]>of(new ByteArray[0]);
    for (final Lengths componentLengths : lengths) {
      final List<ByteArray[]> extended = new ArrayList<>();
      for (final ByteArray[] combination : combinations) {
        for (final ByteArray value : allValues(componentLengths, alphabet)) {
          final ByteArray[] next = Arrays.copyOf(combination, combination.length + 1);
          next[combination.length] = value;
          extended.add(next);
        }
      }
      combinations = extended;
    }
    return combinations;
  }

  private static String describe(final ByteArrayRange range) {
    return Arrays.toString(range.getStart()) + ".." + Arrays.toString(range.getEnd());
  }

  static String describe(final ByteArrayConstraints[] constraints) {
    final StringBuilder description = new StringBuilder();
    for (final ByteArrayConstraints c : constraints) {
      description.append(" {bins=").append(Arrays.toString(c.getBins()));
      if (c.isPrefix()) {
        description.append(" prefix");
      }
      description.append(" ranges=");
      for (final ByteArrayRange r : c.getBinRanges()) {
        description.append(Arrays.toString(r.getStart())).append("..").append(
            Arrays.toString(r.getEnd())).append(' ');
      }
      description.append('}');
    }
    return description.toString();
  }

  private static List<ByteArray> allValues(final Lengths lengths, final byte[] alphabet) {
    final List<ByteArray> all = new ArrayList<>();
    List<ByteArray> values = List.of(new ByteArray(new byte[0]));
    for (int length = 0; length <= lengths.max; length++) {
      if (!lengths.fixed || (length == lengths.max)) {
        all.addAll(values);
      }
      final List<ByteArray> longer = new ArrayList<>();
      for (final ByteArray value : values) {
        for (final byte b : alphabet) {
          longer.add(new ByteArray(ByteArrayUtils.combineArrays(value.getBytes(), new byte[] {b})));
        }
      }
      values = longer;
    }
    return all;
  }

  private static ByteArray[] randomBins(
      final Random random,
      final int count,
      final byte[] alphabet,
      final IntSupplier length) {
    final ByteArray[] bins = new ByteArray[count];
    for (int i = 0; i < count; i++) {
      bins[i] = new ByteArray(randomBytes(random, length.getAsInt(), alphabet));
    }
    return bins;
  }

  private static byte[] randomRangeBound(
      final Random random,
      final byte[] alphabet,
      final IntSupplier length) {
    return random.nextInt(5) == 0 ? null : randomBytes(random, length.getAsInt(), alphabet);
  }

  private static byte[] randomBytes(final Random random, final int length, final byte[] alphabet) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = alphabet[random.nextInt(alphabet.length)];
    }
    return bytes;
  }

  private static byte[] valueId(final byte[] bin) {
    return ByteArrayUtils.combineArrays(VALUE_ID_PREFIX, bin);
  }
}
