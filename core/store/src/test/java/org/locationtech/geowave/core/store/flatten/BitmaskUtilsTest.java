/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.flatten;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class BitmaskUtilsTest {
  static final BitSet zeroth = new BitSet();
  static final BitSet first = new BitSet();
  static final BitSet second = new BitSet();
  static final BitSet third = new BitSet();
  static final BitSet fourth = new BitSet();
  static final BitSet fifth = new BitSet();
  static final BitSet sixth = new BitSet();
  static final BitSet seventh = new BitSet();
  static final BitSet eighth = new BitSet();
  static final BitSet composite_0_1_2 = new BitSet();

  // generate bitsets
  static {
    zeroth.set(0);
    first.set(1);
    second.set(2);
    third.set(3);
    fourth.set(4);
    fifth.set(5);
    sixth.set(6);
    seventh.set(7);
    eighth.set(8);
    composite_0_1_2.set(0);
    composite_0_1_2.set(1);
    composite_0_1_2.set(2);
  }

  @Test
  public void testGenerateBitSet() {
    Assert.assertTrue(
        Arrays.equals(zeroth.toByteArray(), BitmaskUtils.generateCompositeBitmask(0)));
    Assert.assertTrue(
        Arrays.equals(eighth.toByteArray(), BitmaskUtils.generateCompositeBitmask(8)));
  }

  @Test
  public void testByteSize() {

    // confirm bitmasks are of correct (minimal) byte length
    Assert.assertTrue(1 == zeroth.toByteArray().length);
    Assert.assertTrue(2 == eighth.toByteArray().length);
  }

  @Test
  public void testGetOrdinal() {
    List<Integer> positions = BitmaskUtils.getFieldPositions(zeroth.toByteArray());
    Assert.assertTrue(0 == positions.get(0));
    Assert.assertTrue(1 == positions.size());
    positions = BitmaskUtils.getFieldPositions(first.toByteArray());
    Assert.assertTrue(1 == positions.get(0));
    Assert.assertTrue(1 == positions.size());
    positions = BitmaskUtils.getFieldPositions(eighth.toByteArray());
    Assert.assertTrue(8 == positions.get(0));
    Assert.assertTrue(1 == positions.size());
  }

  @Test
  public void testCompositeBitmask() {

    // generate composite bitmask for 3 bitmasks and ensure correctness
    final byte[] bitmask =
        BitmaskUtils.generateCompositeBitmask(new TreeSet<Integer>(Arrays.asList(0, 1, 2)));
    Assert.assertTrue(BitSet.valueOf(bitmask).equals(composite_0_1_2));
  }

  @Test
  public void testDecompositionOfComposite() {

    // decompose composite bitmask and ensure correctness
    final List<Integer> positions = BitmaskUtils.getFieldPositions(composite_0_1_2.toByteArray());
    Assert.assertTrue(positions.size() == 3);
    Assert.assertTrue(0 == positions.get(0));
    Assert.assertTrue(1 == positions.get(1));
    Assert.assertTrue(2 == positions.get(2));
  }

  @Test
  public void testCompositeSortOrder() {

    // generate meaningless fieldInfo to transform
    final Object original = new Object();

    // clone original fieldInfo overwriting dataValue.id with bitmask
    final Pair<Integer, ?> field0 = new ImmutablePair(0, original);
    final Pair<Integer, ?> field1 = new ImmutablePair(1, original);
    final Pair<Integer, ?> field2 = new ImmutablePair(2, original);
    final Pair<Integer, ?> field3 = new ImmutablePair(3, original);
    final Pair<Integer, ?> field4 = new ImmutablePair(4, original);
    final Pair<Integer, ?> field5 = new ImmutablePair(5, original);
    final Pair<Integer, ?> field6 = new ImmutablePair(6, original);
    final Pair<Integer, ?> field7 = new ImmutablePair(7, original);
    final Pair<Integer, ?> field8 = new ImmutablePair(8, original);

    // construct list in wrong order
    final List<Pair<Integer, ?>> fieldInfoList =
        Arrays.asList(field8, field7, field6, field5, field4, field3, field2, field1, field0);

    // sort in place and ensure list sorts correctly
    Collections.sort(fieldInfoList, new BitmaskedPairComparator());

    Assert.assertTrue(field0.equals(fieldInfoList.get(0)));
    Assert.assertTrue(field1.equals(fieldInfoList.get(1)));
    Assert.assertTrue(field2.equals(fieldInfoList.get(2)));
    Assert.assertTrue(field3.equals(fieldInfoList.get(3)));
    Assert.assertTrue(field4.equals(fieldInfoList.get(4)));
    Assert.assertTrue(field5.equals(fieldInfoList.get(5)));
    Assert.assertTrue(field6.equals(fieldInfoList.get(6)));
    Assert.assertTrue(field7.equals(fieldInfoList.get(7)));
    Assert.assertTrue(field8.equals(fieldInfoList.get(8)));
  }
}
