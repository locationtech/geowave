/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArrayRange;

// Data queries, splits and BatchDeleter ranges share this conversion with the metadata reader.
public class AccumuloUtilsTest {
  private static byte[] bytes(final int... values) {
    final byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return bytes;
  }

  private static Range convert(final byte[] start, final byte[] end) {
    return AccumuloUtils.byteArrayRangeToAccumuloRange(new ByteArrayRange(start, end));
  }

  private static boolean contains(final Range range, final int... row) {
    return range.contains(new Key(new Text(bytes(row))));
  }

  @Test
  public void ordinaryRangeEndsAtTheNextPrefixOfItsEnd() {
    final Range range = convert(bytes(0x01), bytes(0x03));
    assertTrue(contains(range, 0x01));
    assertTrue(contains(range, 0x03, 0xFF));
    assertFalse(contains(range, 0x04));
    assertFalse(contains(range, 0x00, 0xFF));
  }

  @Test
  public void endThatIsAPrefixOfTheStartEndsAtItsNextPrefix() {
    final Range range = convert(bytes(0x01, 0x02), bytes(0x01));
    assertTrue(contains(range, 0x01, 0x02));
    assertTrue(contains(range, 0x01, 0xFF, 0xFF));
    assertFalse(contains(range, 0x01, 0x01));
    assertFalse(contains(range, 0x02));
    assertFalse(range.isInfiniteStopKey());
  }

  @Test
  public void endOfAllOnesIsOpen() {
    final Range range = convert(bytes(0xFF, 0x01), bytes(0xFF));
    assertTrue(range.isInfiniteStopKey());
    assertTrue(contains(range, 0xFF, 0x01));
    assertTrue(contains(range, 0xFF, 0xFF, 0xFF));
    assertFalse(contains(range, 0xFF, 0x00));
  }

  @Test
  public void nullStartIsOpen() {
    final Range range = convert(null, bytes(0x05));
    assertTrue(range.isInfiniteStartKey());
    assertTrue(contains(range, 0x00));
    assertTrue(contains(range, 0x05, 0x01));
    assertFalse(contains(range, 0x06));
  }

  @Test
  public void invertedRangeIsDropped() {
    assertNull(convert(bytes(0x03), bytes(0x01)));
    assertNull(convert(bytes(0x01, 0x02), bytes(0x00, 0xFF)));
    assertTrue(
        AccumuloUtils.byteArrayRangesToBoundedAccumuloRanges(
            new ByteArrayRange[] {new ByteArrayRange(bytes(0x03), bytes(0x01))}).isEmpty());
  }
}
