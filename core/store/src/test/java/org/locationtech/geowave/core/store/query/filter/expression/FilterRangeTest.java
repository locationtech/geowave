/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.junit.Test;
import com.google.common.collect.Lists;

public class FilterRangeTest {
  @Test
  public void testMergeIntRanges() {
    List<FilterRange<Integer>> intRanges =
        Lists.newArrayList(
            FilterRange.of(3, 40, true, true, true),
            FilterRange.of(1, 45, true, true, true),
            FilterRange.of(2, 50, true, true, true),
            FilterRange.of(70, 75, true, true, true),
            FilterRange.of(100, 200, true, true, true),
            FilterRange.of(75, 90, true, true, true),
            FilterRange.of(80, 85, true, true, true));

    List<FilterRange<Integer>> merged = FilterRange.mergeRanges(intRanges);
    assertEquals(3, merged.size());
    assertFalse(merged.get(0).isFullRange());
    assertEquals(1, (int) merged.get(0).getStart());
    assertEquals(50, (int) merged.get(0).getEnd());
    assertFalse(merged.get(1).isFullRange());
    assertEquals(70, (int) merged.get(1).getStart());
    assertEquals(90, (int) merged.get(1).getEnd());
    assertFalse(merged.get(2).isFullRange());
    assertEquals(100, (int) merged.get(2).getStart());
    assertEquals(200, (int) merged.get(2).getEnd());

    intRanges =
        Lists.newArrayList(
            FilterRange.of(3, 40, true, true, true),
            FilterRange.of(1, 45, true, true, true),
            FilterRange.of(2, 50, true, true, true),
            FilterRange.of(null, 75, true, true, true),
            FilterRange.of(100, 200, true, true, true),
            FilterRange.of(75, 90, true, true, true),
            FilterRange.of(80, 85, true, true, true));

    merged = FilterRange.mergeRanges(intRanges);
    assertEquals(2, merged.size());
    assertFalse(merged.get(0).isFullRange());
    assertNull(merged.get(0).getStart());
    assertEquals(90, (int) merged.get(0).getEnd());
    assertFalse(merged.get(1).isFullRange());
    assertEquals(100, (int) merged.get(1).getStart());
    assertEquals(200, (int) merged.get(1).getEnd());

    intRanges =
        Lists.newArrayList(
            FilterRange.of(3, 40, true, true, true),
            FilterRange.of(1, 45, true, true, true),
            FilterRange.of(2, 50, true, true, true),
            FilterRange.of(70, null, true, true, true),
            FilterRange.of(100, 200, true, true, true),
            FilterRange.of(75, 90, true, true, true),
            FilterRange.of(80, 85, true, true, true));

    merged = FilterRange.mergeRanges(intRanges);
    assertEquals(2, merged.size());
    assertFalse(merged.get(0).isFullRange());
    assertEquals(1, (int) merged.get(0).getStart());
    assertEquals(50, (int) merged.get(0).getEnd());
    assertFalse(merged.get(1).isFullRange());
    assertEquals(70, (int) merged.get(1).getStart());
    assertNull(merged.get(1).getEnd());

    intRanges =
        Lists.newArrayList(
            FilterRange.of(3, 40, true, true, true),
            FilterRange.of(1, 45, true, true, true),
            FilterRange.of(2, 50, true, true, true),
            FilterRange.of(70, null, true, true, true),
            FilterRange.of(null, 200, true, true, true),
            FilterRange.of(75, 90, true, true, true),
            FilterRange.of(80, 85, true, true, true));

    merged = FilterRange.mergeRanges(intRanges);
    assertEquals(1, merged.size());
    assertTrue(merged.get(0).isFullRange());
    assertNull(merged.get(0).getStart());
    assertNull(merged.get(0).getEnd());
  }

  @Test
  public void testIntersectIntRanges() {
    List<FilterRange<Integer>> intRanges1 =
        Lists.newArrayList(
            FilterRange.of(0, 2, true, true, true),
            FilterRange.of(5, 10, true, true, true),
            FilterRange.of(13, 23, true, true, true),
            FilterRange.of(24, 25, true, true, true));

    List<FilterRange<Integer>> intRanges2 =
        Lists.newArrayList(
            FilterRange.of(1, 5, true, true, true),
            FilterRange.of(8, 12, true, true, true),
            FilterRange.of(15, 18, true, true, true),
            FilterRange.of(20, 24, true, true, true));

    List<FilterRange<Integer>> intersected = FilterRange.intersectRanges(intRanges1, intRanges2);
    assertEquals(6, intersected.size());
    assertEquals(1, (int) intersected.get(0).getStart());
    assertEquals(2, (int) intersected.get(0).getEnd());
    assertEquals(5, (int) intersected.get(1).getStart());
    assertEquals(5, (int) intersected.get(1).getEnd());
    assertEquals(8, (int) intersected.get(2).getStart());
    assertEquals(10, (int) intersected.get(2).getEnd());
    assertEquals(15, (int) intersected.get(3).getStart());
    assertEquals(18, (int) intersected.get(3).getEnd());
    assertEquals(20, (int) intersected.get(4).getStart());
    assertEquals(23, (int) intersected.get(4).getEnd());
    assertEquals(24, (int) intersected.get(5).getStart());
    assertEquals(24, (int) intersected.get(5).getEnd());

    intRanges1 = Lists.newArrayList(FilterRange.of(null, null, true, true, true));

    intersected = FilterRange.intersectRanges(intRanges1, intRanges2);
    assertEquals(4, intersected.size());
    assertEquals(1, (int) intersected.get(0).getStart());
    assertEquals(5, (int) intersected.get(0).getEnd());
    assertEquals(8, (int) intersected.get(1).getStart());
    assertEquals(12, (int) intersected.get(1).getEnd());
    assertEquals(15, (int) intersected.get(2).getStart());
    assertEquals(18, (int) intersected.get(2).getEnd());
    assertEquals(20, (int) intersected.get(3).getStart());
    assertEquals(24, (int) intersected.get(3).getEnd());

    intRanges1 =
        Lists.newArrayList(
            FilterRange.of(0, 2, true, true, true),
            FilterRange.of(5, 10, true, true, true),
            FilterRange.of(13, 23, true, true, true),
            FilterRange.of(24, 25, true, true, true));

    intRanges2 = Lists.newArrayList(FilterRange.of(null, null, true, true, true));

    intersected = FilterRange.intersectRanges(intRanges1, intRanges2);
    assertEquals(4, intersected.size());
    assertEquals(0, (int) intersected.get(0).getStart());
    assertEquals(2, (int) intersected.get(0).getEnd());
    assertEquals(5, (int) intersected.get(1).getStart());
    assertEquals(10, (int) intersected.get(1).getEnd());
    assertEquals(13, (int) intersected.get(2).getStart());
    assertEquals(23, (int) intersected.get(2).getEnd());
    assertEquals(24, (int) intersected.get(3).getStart());
    assertEquals(25, (int) intersected.get(3).getEnd());

    intRanges1 = Lists.newArrayList(FilterRange.of(null, null, true, true, true));
    intRanges2 = Lists.newArrayList(FilterRange.of(null, null, true, true, true));

    intersected = FilterRange.intersectRanges(intRanges1, intRanges2);
    assertEquals(1, intersected.size());
    assertNull(intersected.get(0).getStart());
    assertNull(intersected.get(0).getEnd());

    intRanges1 =
        Lists.newArrayList(
            FilterRange.of(1, 5, true, true, true),
            FilterRange.of(8, 10, true, true, true));
    intRanges2 =
        Lists.newArrayList(
            FilterRange.of(15, 18, true, true, true),
            FilterRange.of(20, 24, true, true, true));

    intersected = FilterRange.intersectRanges(intRanges1, intRanges2);
    assertEquals(0, intersected.size());
  }

  @Test
  public void testMergeStringRanges() {}
}
