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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.statistics.binning.BinConstraintMatching.Kind;
import org.locationtech.geowave.core.store.statistics.binning.BinConstraintMatching.Lengths;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

public class FieldValueBinningStrategyTest {
  // single-byte UTF-8 characters, including the second byte of the 2-byte '|' separator but not
  // its first, 0x00, which field values must not contain
  private static final byte[] ALPHABET = {0x01, '|', 0x7F};
  private static final DataTypeAdapter<Entry> ADAPTER =
      BasicDataTypeAdapter.newAdapter("entries", Entry.class, "id");
  private static final byte[] SEPARATOR = {0, '|'};

  @Test
  public void combinedConstraintsMatchExactlyWhatTheirFieldConstraintsMatchOrAreRefused() {
    final Random random = new Random(20261103);
    final Map<String, List<Pair<ByteArray[], ByteArray>>> binsByFieldCount = new HashMap<>();
    final Map<String, Integer> outcomes = new TreeMap<>();
    for (int iteration = 0; iteration < 3000; iteration++) {
      final Lengths[] lengths =
          random.nextBoolean() ? new Lengths[] {new Lengths(2, false), new Lengths(2, false)}
              : new Lengths[] {new Lengths(2, false), new Lengths(1, false), new Lengths(2, false)};
      final String[] fields = Arrays.copyOf(new String[] {"a", "b", "c"}, lengths.length);
      final FieldValueBinningStrategy strategy = new FieldValueBinningStrategy(fields);
      final Kind[] kinds = new Kind[lengths.length];
      final ByteArrayConstraints[] fieldConstraints = new ByteArrayConstraints[lengths.length];
      final Map<String, Object> constraints = new HashMap<>();
      for (int i = 0; i < lengths.length; i++) {
        kinds[i] = BinConstraintMatching.randomKind(random);
        fieldConstraints[i] =
            BinConstraintMatching.randomConstraints(random, kinds[i], lengths[i], ALPHABET);
        constraints.put(fields[i], fieldConstraints[i]);
      }
      final String description =
          Arrays.toString(kinds) + BinConstraintMatching.describe(fieldConstraints);
      final String refusal = expectedRefusal(kinds, fieldConstraints);
      if (refusal != null) {
        assertThrows(
            description,
            IllegalArgumentException.class,
            () -> strategy.constraints(constraints));
        outcomes.merge(refusal, 1, Integer::sum);
        continue;
      }
      final ByteArrayConstraints combined = strategy.constraints(constraints);
      outcomes.merge(
          combined.isAllBins() ? "all"
              : combined.getBins().length > 0 ? (combined.isPrefix() ? "prefix" : "exact")
                  : combined.getBinRanges().length > 0 ? "ranges" : "empty",
          1,
          Integer::sum);
      BinConstraintMatching.assertDisjoint(combined.getBinRanges());
      for (final Pair<ByteArray[], ByteArray> valuesAndBin : binsByFieldCount.computeIfAbsent(
          Arrays.toString(lengths),
          k -> allBins(strategy, lengths))) {
        final ByteArray[] values = valuesAndBin.getLeft();
        boolean expected = true;
        for (int i = 0; i < values.length; i++) {
          expected &= componentMatches(fieldConstraints[i], values[i]);
        }
        assertEquals(
            description + " on " + Arrays.toString(values),
            expected,
            readPathMatches(combined, valuesAndBin.getRight()));
      }
    }
    for (final String outcome : new String[] {
        "all",
        "exact",
        "prefix",
        "ranges",
        "empty",
        "illegal order",
        "exact bins with ranges on the last field"}) {
      assertTrue(outcome + " never produced: " + outcomes, outcomes.containsKey(outcome));
    }
  }

  @Test
  public void exactThenAllBinsIsASeparatorTerminatedPrefix() {
    final ByteArrayConstraints combined =
        new FieldValueBinningStrategy("a", "b", "c").constraints(
            new Pair[] {
                Pair.of("a", new String[] {"x", "y"}),
                Pair.of("b", BinConstraints.allBins()),
                Pair.of("c", BinConstraints.allBins())});
    assertTrue(combined.isPrefix());
    assertArrayEquals(
        new ByteArray[] {
            new ByteArray(bytes("x", SEPARATOR)),
            new ByteArray(bytes("y", SEPARATOR))},
        combined.getBins());
  }

  @Test
  public void exactThenPrefixHasNoTrailingSeparator() {
    final ByteArrayConstraints combined =
        new FieldValueBinningStrategy("a", "b").constraints(
            Map.of("a", "x", "b", BinConstraints.ofPrefix(new ByteArray("y"))));
    assertTrue(combined.isPrefix());
    assertArrayEquals(
        new ByteArray[] {new ByteArray(bytes("x", SEPARATOR, "y"))},
        combined.getBins());
  }

  @Test
  public void exactThenRangesJoinsEachEnd() {
    final ByteArrayConstraints combined =
        new FieldValueBinningStrategy("a", "b").constraints(
            Map.of(
                "a",
                "x",
                "b",
                new ByteArrayRange[] {
                    new ByteArrayRange(bytes("c"), bytes("f")),
                    new ByteArrayRange(null, bytes("b"))}));
    assertArrayEquals(
        new ByteArrayRange[] {
            new ByteArrayRange(bytes("x", SEPARATOR), bytes("x", SEPARATOR, "b")),
            new ByteArrayRange(bytes("x", SEPARATOR, "c"), bytes("x", SEPARATOR, "f"))},
        combined.getBinRanges());
  }

  @Test
  public void exactBinsAlongsideRangesStayExact() {
    final FieldValueBinningStrategy strategy = new FieldValueBinningStrategy("a", "b");
    final ExplicitConstraints abOrXToY =
        new ExplicitConstraints(
            new ByteArray[] {new ByteArray("ab")},
            new ByteArrayRange[] {new ByteArrayRange(bytes("x"), bytes("y"))},
            false,
            false);
    final ByteArrayConstraints combined =
        strategy.constraints(Map.of("a", abOrXToY, "b", BinConstraints.allBins()));
    assertArrayEquals(
        new ByteArrayRange[] {
            new ByteArrayRange(bytes("ab", SEPARATOR), bytes("ab", SEPARATOR)),
            new ByteArrayRange(bytes("x"), bytes("y"))},
        combined.getBinRanges());
    assertTrue(readPathMatches(combined, strategy.getBins(ADAPTER, entry("ab", "z"))[0]));
    assertTrue(readPathMatches(combined, strategy.getBins(ADAPTER, entry("xy", "z"))[0]));
    assertFalse(readPathMatches(combined, strategy.getBins(ADAPTER, entry("abc", "z"))[0]));

    // a range end matches everything that starts with it, so it cannot stop exactly at "ab" when
    // nothing follows the field
    assertThrows(
        IllegalArgumentException.class,
        () -> strategy.constraints(Map.of("a", "K", "b", abOrXToY)));
  }

  @Test
  public void constraintsThatCannotBeKeyRangesAreRejected() {
    for (final Map<String, Object> constraints : List.<Map<String, Object>>of(
        Map.of("a", BinConstraints.allBins(), "b", "y"),
        Map.of("a", BinConstraints.ofPrefix(new ByteArray("x")), "b", "y"),
        Map.of("a", new ByteArrayRange(bytes("x"), bytes("y")), "b", "y"))) {
      assertThrows(
          constraints.toString(),
          IllegalArgumentException.class,
          () -> new FieldValueBinningStrategy("a", "b").constraints(constraints));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new FieldValueBinningStrategy("a", "b", "c").constraints(
            Map.of("a", "x", "b", BinConstraints.allBins(), "c", "z")));
  }

  @Test
  public void numericRangeFieldsCanBeFollowedByAllBins() {
    final NumericRangeFieldValueBinningStrategy single =
        new NumericRangeFieldValueBinningStrategy("x");
    assertTrue(single.constraints(BinConstraints.allBins()).isAllBins());

    final NumericRangeFieldValueBinningStrategy multi =
        new NumericRangeFieldValueBinningStrategy("x", "y");
    final ByteArrayConstraints combined =
        multi.constraints(Map.of("x", 5, "y", BinConstraints.allBins()));
    assertTrue(combined.isPrefix());
    assertTrue(readPathMatches(combined, multi.getBins(ADAPTER, entry(5.5, 7))[0]));
    assertTrue(readPathMatches(combined, multi.getBins(ADAPTER, entry(5, -300))[0]));
    assertFalse(readPathMatches(combined, multi.getBins(ADAPTER, entry(6.5, 7))[0]));
  }

  public static class Entry {
    private String id;
    private String a;
    private String b;
    private String c;
    private double x;
    private double y;

    public Entry() {}

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public String getA() {
      return a;
    }

    public void setA(final String a) {
      this.a = a;
    }

    public String getB() {
      return b;
    }

    public void setB(final String b) {
      this.b = b;
    }

    public String getC() {
      return c;
    }

    public void setC(final String c) {
      this.c = c;
    }

    public double getX() {
      return x;
    }

    public void setX(final double x) {
      this.x = x;
    }

    public double getY() {
      return y;
    }

    public void setY(final double y) {
      this.y = y;
    }
  }

  private static String expectedRefusal(
      final Kind[] kinds,
      final ByteArrayConstraints[] constraints) {
    if (!BinConstraintMatching.isLegal(kinds)) {
      return "illegal order";
    }
    final int last = kinds.length - 1;
    for (int i = 0; i < last; i++) {
      if (kinds[i] != Kind.EXACT) {
        return null;
      }
    }
    return (kinds[last] == Kind.RANGES) && (constraints[last].getBins().length > 0)
        ? "exact bins with ranges on the last field"
        : null;
  }

  private static List<Pair<ByteArray[], ByteArray>> allBins(
      final FieldValueBinningStrategy strategy,
      final Lengths[] lengths) {
    final List<Pair<ByteArray[], ByteArray>> bins = new ArrayList<>();
    for (final ByteArray[] values : BinConstraintMatching.allCombinations(lengths, ALPHABET)) {
      final Entry entry = new Entry();
      entry.setA(new String(values[0].getBytes(), StandardCharsets.UTF_8));
      entry.setB(new String(values[1].getBytes(), StandardCharsets.UTF_8));
      if (values.length > 2) {
        entry.setC(new String(values[2].getBytes(), StandardCharsets.UTF_8));
      }
      final ByteArray[] entryBins = strategy.getBins(ADAPTER, entry);
      assertEquals(1, entryBins.length);
      bins.add(Pair.of(values, entryBins[0]));
    }
    return bins;
  }

  private static Entry entry(final String a, final String b) {
    final Entry entry = new Entry();
    entry.setA(a);
    entry.setB(b);
    return entry;
  }

  private static Entry entry(final double x, final double y) {
    final Entry entry = new Entry();
    entry.setX(x);
    entry.setY(y);
    return entry;
  }

  private static byte[] bytes(final Object... parts) {
    byte[] bytes = new byte[0];
    for (final Object part : parts) {
      bytes =
          ByteArrayUtils.combineArrays(
              bytes,
              part instanceof byte[] ? (byte[]) part : StringUtils.stringToBinary((String) part));
    }
    return bytes;
  }
}
