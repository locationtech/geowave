/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayRange.MergeOperation;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import com.google.common.primitives.Bytes;

/**
 * Static utility methods useful for binning strategies
 *
 */
public class BinningStrategyUtils {
  static final ByteArray EMPTY = new ByteArray(new byte[0]);

  /**
   * in the case of queries of composite or multi-field bins, we need to query all combinations of
   * individual bins
   *
   * @param individualBins the individual bin
   * @param concatenateByteArrays a method to concatenate the bins, useful for different nuances of
   *        encoding
   * @return all combinations of the concatenated individual bins
   */
  public static ByteArray[] getAllCombinations(
      final ByteArray[][] individualBins,
      final Function<ByteArray[], ByteArray> concatenateByteArrays) {
    return combinations(individualBins).stream().map(concatenateByteArrays).toArray(
        ByteArray[]::new);
  }

  /**
   * @param bins the bins to concatenate
   * @return the bytes of the bins concatenated without any separator
   */
  public static ByteArray concatenate(final ByteArray[] bins) {
    return new ByteArray(
        Bytes.concat(Arrays.stream(bins).map(ByteArray::getBytes).toArray(byte[][]::new)));
  }

  /**
   * The layout of a bin made of several components, such as a composite or multi-field bin, as
   * {@link #combineConstraints} needs it.
   */
  interface ComponentLayout {
    /** @return the whole bin made of one bin of every component */
    ByteArray getBin(ByteArray[] componentBins);

    /**
     * @return the key prefix of the bins whose leading components are the given bins and whose next
     *         component starts with the partial bin; an empty partial bin matches any bin of it
     */
    ByteArray getPrefix(ByteArray[] leadingBins, ByteArray partialBin);

    /**
     * @return the key prefix of exactly the bins whose leading components are the given bins and
     *         whose next component is the given bin, for a component that {@link #checkDelimited}
     *         accepted with exact bins alongside ranges
     */
    ByteArray getExactPrefix(ByteArray[] leadingBins, ByteArray bin);

    /**
     * Check that a key prefix can stop exactly at the end of a component, so that the component can
     * lead prefixes and ranges or be constrained by them, and that its constraints fit it.
     *
     * @throws IllegalArgumentException if it cannot or they do not
     */
    void checkDelimited(int component, ByteArrayConstraints constraints);

    /** @return the component at an index, as error messages name it */
    String describe(int component);
  }

  /**
   * Combine the constraints on each component of a bin made of several components into constraints
   * on the whole bin. Exact leading components multiply out. The first component that is not exact
   * may be constrained to all bins, prefixes or ranges, which become prefixes or ranges under every
   * leading combination, and every later component must then be constrained to all bins. Prefixes
   * and ranges need the layout to delimit the leading components and the constrained one. An exact
   * component with no bins matches nothing.
   *
   * @return the constraints on the whole bin
   * @throws IllegalArgumentException if a component that is not constrained to all bins follows one
   *         constrained to all bins, prefixes or ranges, or if the layout cannot delimit a
   *         component that prefixes or ranges need
   */
  static ByteArrayConstraints combineConstraints(
      final List<ByteArrayConstraints> componentConstraints,
      final ComponentLayout layout) {
    final int componentCount = componentConstraints.size();
    final int firstInexact =
        IntStream.range(0, componentCount).filter(
            i -> !isExact(componentConstraints.get(i))).findFirst().orElse(componentCount);
    for (int i = firstInexact + 1; i < componentCount; i++) {
      if (!componentConstraints.get(i).isAllBins()) {
        throw new IllegalArgumentException(
            "Cannot constrain "
                + layout.describe(i)
                + " after the 'all bins', prefix or range constraint on "
                + layout.describe(firstInexact)
                + "; only 'all bins' may follow such a constraint");
      }
    }
    final ByteArray[][] leadingBins =
        componentConstraints.subList(0, firstInexact).stream().map(
            ByteArrayConstraints::getBins).toArray(ByteArray[][]::new);
    if (firstInexact == componentCount) {
      return new ExplicitConstraints(getAllCombinations(leadingBins, layout::getBin));
    }
    final ByteArrayConstraints partial = componentConstraints.get(firstInexact);
    if (partial.isAllBins() && (firstInexact == 0)) {
      return new ExplicitConstraints(true);
    }
    for (int i = 0; i < firstInexact; i++) {
      layout.checkDelimited(i, componentConstraints.get(i));
    }
    if (!partial.isAllBins()) {
      layout.checkDelimited(firstInexact, partial);
    }
    final List<ByteArray[]> leadingCombinations = combinations(leadingBins);
    if (leadingCombinations.isEmpty()) {
      return new ExplicitConstraints();
    }
    if (partial.isAllBins() || (partial.getBinRanges().length == 0)) {
      final ByteArray[] prefixes =
          partial.isAllBins() ? new ByteArray[] {EMPTY} : partial.getBins();
      return new ExplicitConstraints(
          leadingCombinations.stream().flatMap(
              l -> Arrays.stream(prefixes).map(p -> layout.getPrefix(l, p))).toArray(
                  ByteArray[]::new),
          true);
    }
    final List<ByteArrayRange> ranges = new ArrayList<>();
    for (final ByteArray[] leading : leadingCombinations) {
      for (final ByteArray bin : partial.getBins()) {
        final byte[] prefix =
            (partial.isPrefix() ? layout.getPrefix(leading, bin)
                : layout.getExactPrefix(leading, bin)).getBytes();
        ranges.add(new ByteArrayRange(prefix, prefix));
      }
      for (final ByteArrayRange range : partial.getBinRanges()) {
        // a null bound stays under the leading bins instead of spanning the whole key space
        ranges.add(
            new ByteArrayRange(
                layout.getPrefix(leading, orEmpty(range.getStart())).getBytes(),
                layout.getPrefix(leading, orEmpty(range.getEnd())).getBytes()));
      }
    }
    // stores scan each range separately, so overlapping ranges would read a bin more than once; an
    // empty range (start past end) would keep the ranges around it from merging
    ranges.removeIf(r -> ByteArrayUtils.compare(r.getStart(), r.getEndAsNextPrefix()) >= 0);
    return new ExplicitConstraints(
        ByteArrayRange.mergeIntersections(ranges, MergeOperation.UNION).toArray(
            new ByteArrayRange[0]));
  }

  /**
   * Check that the constraints on a component whose bins all have one length fit that length: exact
   * bins have it, and prefixes and range bounds are no longer.
   *
   * @throws IllegalArgumentException if they do not
   */
  static void checkFixedLength(
      final int length,
      final ByteArrayConstraints constraints,
      final String component) {
    if (!constraints.isPrefix()) {
      for (final ByteArray bin : constraints.getBins()) {
        if (bin.getBytes().length != length) {
          throw new IllegalArgumentException(
              "Cannot use the exact bin "
                  + bin.getHexString()
                  + " for "
                  + component
                  + ": its bins are "
                  + length
                  + " bytes");
        }
      }
    } else {
      Arrays.stream(constraints.getBins()).forEach(
          b -> checkNotLonger(length, b.getBytes(), "prefix", component));
    }
    for (final ByteArrayRange range : constraints.getBinRanges()) {
      checkNotLonger(length, range.getStart(), "range start", component);
      checkNotLonger(length, range.getEnd(), "range end", component);
    }
  }

  private static void checkNotLonger(
      final int length,
      final byte[] bytes,
      final String what,
      final String component) {
    if ((bytes != null) && (bytes.length > length)) {
      throw new IllegalArgumentException(
          "Cannot use the "
              + what
              + " "
              + new ByteArray(bytes).getHexString()
              + " for "
              + component
              + ": it is longer than its "
              + length
              + "-byte bins");
    }
  }

  private static boolean isExact(final ByteArrayConstraints constraints) {
    return !constraints.isAllBins()
        && !constraints.isPrefix()
        && (constraints.getBinRanges().length == 0);
  }

  private static ByteArray orEmpty(final byte[] bytes) {
    return bytes == null ? EMPTY : new ByteArray(bytes);
  }

  private static List<ByteArray[]> combinations(final ByteArray[][] individualBins) {
    final List<ByteArray[]> combinations = new ArrayList<>();
    combos(0, individualBins, new ByteArray[0], combinations);
    return combinations;
  }

  private static void combos(
      final int pos,
      final ByteArray[][] c,
      final ByteArray[] soFar,
      final List<ByteArray[]> finalList) {
    if (pos == c.length) {
      finalList.add(soFar);
      return;
    }
    for (int i = 0; i != c[pos].length; i++) {
      combos(pos + 1, c, (ByteArray[]) ArrayUtils.add(soFar, c[pos][i]), finalList);
    }
  }
}
