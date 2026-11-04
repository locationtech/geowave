/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloUtils;

public class AccumuloMetadataRangesTest {
  // as StatisticValue.getValueId: statistic id, separator, bin
  private static final String PREFIX = "COUNT|alpha|";
  private static final byte[] GROUP = "Atype".getBytes(StandardCharsets.UTF_8);

  private static byte[] id(final String bin) {
    return (PREFIX + bin).getBytes(StandardCharsets.UTF_8);
  }

  private static ByteArrayRange range(final String startBin, final String endBin) {
    return new ByteArrayRange(
        id(startBin == null ? "" : startBin),
        id(endBin == null ? "" : endBin));
  }

  private static boolean inRange(final Range range, final String row) {
    return range.contains(new Key(new Text(row)));
  }

  @Test
  public void openEndedRangeStaysWithinTheStatistic() {
    // a null bin end is mapped to the bare statistic prefix
    final List<Range> ranges =
        AccumuloUtils.byteArrayRangesToBoundedAccumuloRanges(
            new ByteArrayRange[] {range("b05", null)});
    assertEquals(1, ranges.size());
    final Range open = ranges.get(0);
    assertFalse(open.isInfiniteStopKey());
    assertTrue(inRange(open, PREFIX + "b05"));
    assertTrue(inRange(open, PREFIX + "b99"));
    assertFalse(inRange(open, "COUNT|beta|b05"));
    assertFalse(inRange(open, PREFIX + "b04"));
  }

  @Test
  public void openStartedRangeStaysWithinTheStatistic() {
    // a null bin start is mapped to the bare statistic prefix
    final List<Range> ranges =
        AccumuloUtils.byteArrayRangesToBoundedAccumuloRanges(
            new ByteArrayRange[] {range(null, "b02")});
    assertEquals(1, ranges.size());
    final Range open = ranges.get(0);
    assertFalse(open.isInfiniteStartKey());
    assertTrue(inRange(open, PREFIX + "b01"));
    assertTrue(inRange(open, PREFIX + "b02x"));
    assertFalse(inRange(open, PREFIX + "b03"));
    assertFalse(inRange(open, "COUNT|alpha"));
    assertFalse(inRange(open, "COUNT|aaa|b01"));
  }

  @Test
  public void invertedRangesAreDroppedNotWidened() {
    assertTrue(
        AccumuloUtils.byteArrayRangesToBoundedAccumuloRanges(
            new ByteArrayRange[] {range("b04", "b02")}).isEmpty());
    // an end that is a prefix of the start is not inverted
    assertEquals(
        1,
        AccumuloUtils.byteArrayRangesToBoundedAccumuloRanges(
            new ByteArrayRange[] {range("b04x", "b04")}).size());
  }

  @Test
  public void deleterSkipsInvertedRangesAndBoundsOpenOnes() {
    final AccumuloOperations operations = EasyMock.createMock(AccumuloOperations.class);
    final Capture<Collection<Range>> captured = EasyMock.newCapture();
    EasyMock.expect(
        operations.deleteRanges(
            EasyMock.anyString(),
            EasyMock.capture(captured),
            EasyMock.anyString(),
            EasyMock.aryEq(GROUP))).andReturn(true).once();
    EasyMock.replay(operations);
    final AccumuloMetadataDeleter deleter =
        new AccumuloMetadataDeleter(operations, MetadataType.STATISTIC_VALUES);

    // an inverted range must not reach the operations (any call would fail the strict mock)
    assertFalse(
        deleter.delete(new MetadataQuery(new ByteArrayRange[] {range("b04", "b02")}, GROUP)));
    assertTrue(
        deleter.delete(
            new MetadataQuery(
                new ByteArrayRange[] {range("b05", null), range("b04", "b02")},
                GROUP)));
    final Collection<Range> ranges = captured.getValue();
    assertEquals(1, ranges.size());
    final Range range = ranges.iterator().next();
    assertFalse(range.isInfiniteStopKey());
    assertFalse(range.isInfiniteStartKey());
    assertFalse(inRange(range, "COUNT|beta|b05"));
    assertTrue(inRange(range, PREFIX + "b06"));
    EasyMock.verify(operations);
  }

  @Test
  public void readerReturnsNothingForInvertedRanges() {
    // no scanner may be created, which the mock enforces by having no expectations
    final AccumuloOperations operations = EasyMock.createMock(AccumuloOperations.class);
    EasyMock.replay(operations);
    final AccumuloMetadataReader reader =
        new AccumuloMetadataReader(
            operations,
            new BaseDataStoreOptions(),
            MetadataType.STATISTIC_VALUES);
    assertFalse(
        reader.query(
            new MetadataQuery(new ByteArrayRange[] {range("b04", "b02")}, GROUP)).hasNext());
    EasyMock.verify(operations);
  }
}
