/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;

/**
 * DynamoDB compares binary key attributes as unsigned bytes, so a query's BETWEEN bounds have to
 * bracket the range keys {@link DynamoDBRow#getRangeKey} writes under that same ordering. These
 * exercise the case that used to drop rows: a sort key whose length is a multiple of three, where
 * the encoded form of a padded sort key keeps the unpadded form as a clean prefix.
 */
public class DynamoDBRangeBoundsTest {
  private static final short ADAPTER_ID = 1;
  private static final byte[] PARTITION = new byte[] {7};

  private static byte[] storedRangeKey(final byte[] sortKey, final byte[] dataId) {
    return DynamoDBRow.getRangeKey(new GeoWaveKeyImpl(dataId, ADAPTER_ID, PARTITION, sortKey, 0));
  }

  /** Unsigned lexicographic comparison, which is how DynamoDB orders binary keys. */
  private static int compareUnsigned(final byte[] a, final byte[] b) {
    for (int i = 0; (i < a.length) && (i < b.length); i++) {
      final int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
      if (diff != 0) {
        return diff;
      }
    }
    return a.length - b.length;
  }

  private static boolean within(final byte[] key, final byte[] start, final byte[] end) {
    return (compareUnsigned(key, start) >= 0) && (compareUnsigned(key, end) <= 0);
  }

  @Test
  public void everyDataIdFallsWithinTheBoundsForItsSortKey() {
    // Lengths either side of a multiple of three, because that is what decides whether the encoded
    // padding lands in its own base64 group or merges into the previous one.
    for (int sortKeyLength = 6; sortKeyLength <= 18; sortKeyLength++) {
      final byte[] sortKey = new byte[sortKeyLength];
      for (int i = 0; i < sortKeyLength; i++) {
        sortKey[i] = (byte) i;
      }
      final byte[] start = DynamoDBReader.rangeStart(ADAPTER_ID, sortKey);
      final byte[] end = DynamoDBReader.singleValueRangeEnd(ADAPTER_ID, sortKey);

      // Every possible leading dataId byte, including the 133 values above 'z' (0x7A) that the
      // encoding can never produce and that therefore used to sort past the end bound.
      for (int leadingByte = 0; leadingByte <= 0xFF; leadingByte++) {
        final byte[] dataId = new byte[] {(byte) leadingByte, 0x00, 0x00, 0x00};
        final byte[] key = storedRangeKey(sortKey, dataId);
        assertTrue(
            "sort key length "
                + sortKeyLength
                + " lost the row whose dataId begins 0x"
                + Integer.toHexString(leadingByte),
            within(key, start, end));
      }
    }
  }

  @Test
  public void aHigherSortKeyFallsOutsideTheBounds() {
    for (int sortKeyLength = 6; sortKeyLength <= 18; sortKeyLength++) {
      final byte[] sortKey = new byte[sortKeyLength];
      final byte[] higher = new byte[sortKeyLength];
      for (int i = 0; i < sortKeyLength; i++) {
        sortKey[i] = (byte) i;
        higher[i] = (byte) i;
      }
      higher[sortKeyLength - 1] = (byte) (sortKey[sortKeyLength - 1] + 1);

      final byte[] end = DynamoDBReader.singleValueRangeEnd(ADAPTER_ID, sortKey);
      final byte[] key = storedRangeKey(higher, new byte[] {0x00, 0x00, 0x00, 0x00});
      assertTrue(
          "sort key length " + sortKeyLength + " would have included the next sort key up",
          compareUnsigned(key, end) > 0);
    }
  }

  @Test
  public void aLowerSortKeyFallsOutsideTheBounds() {
    final byte[] sortKey = new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    final byte[] lower = sortKey.clone();
    lower[lower.length - 1] = 0x07;

    final byte[] start = DynamoDBReader.rangeStart(ADAPTER_ID, sortKey);
    final byte[] key = storedRangeKey(lower, new byte[] {(byte) 0xFF, 0x00, 0x00, 0x00});
    assertTrue(compareUnsigned(key, start) < 0);
  }

  /**
   * A text search asks for a range whose end is a prefix of the keys it should match, so the end
   * bound has to keep including stored keys that are longer than it. Padding after the encoding --
   * which is what {@link DynamoDBReader#singleValueRangeEnd} does and what this bound must not do
   * -- loses 73% of them, because the encoded end stops at a '=' pad that sorts below the real
   * character a longer key has in that position.
   *
   * <p> Only the end bound is asserted here. The <em>start</em> bound has the mirror image of the
   * same defect and it is not fixable without changing the encoding: encodeSortableBase64 maps
   * base64's '=' pad to 0x3D, which sits between '9' and 'A' rather than below everything, so a
   * short sort key can encode above a longer one that extends it. That is a pre-existing hole in
   * prefix search on this store, not something this change introduces.
   */
  @Test
  public void aRangeEndStillCoversLongerSortKeysThatStartWithIt() {
    for (int prefixLength = 1; prefixLength <= 12; prefixLength++) {
      final byte[] prefix = new byte[prefixLength];
      for (int i = 0; i < prefixLength; i++) {
        prefix[i] = (byte) (i + 1);
      }
      final byte[] end = DynamoDBReader.rangeEnd(ADAPTER_ID, prefix);

      for (int suffixLength = 1; suffixLength <= 4; suffixLength++) {
        for (int suffixByte = 0; suffixByte <= 0xFF; suffixByte += 17) {
          final byte[] longer = new byte[prefixLength + suffixLength];
          System.arraycopy(prefix, 0, longer, 0, prefixLength);
          for (int i = prefixLength; i < longer.length; i++) {
            longer[i] = (byte) suffixByte;
          }
          final byte[] key = storedRangeKey(longer, new byte[] {0x00, 0x00, 0x00, 0x00});
          assertTrue(
              "prefix length "
                  + prefixLength
                  + " lost a sort key extended by "
                  + suffixLength
                  + " bytes of 0x"
                  + Integer.toHexString(suffixByte),
              compareUnsigned(key, end) <= 0);
        }
      }
    }
  }

  @Test
  public void anAdapterWideRangeCoversEverySortKey() {
    final byte[] start = DynamoDBReader.rangeStart(ADAPTER_ID, null);
    final byte[] end = DynamoDBReader.rangeEnd(ADAPTER_ID, null);
    for (int sortKeyLength = 1; sortKeyLength <= 18; sortKeyLength++) {
      final byte[] sortKey = new byte[sortKeyLength];
      for (int i = 0; i < sortKeyLength; i++) {
        sortKey[i] = (byte) (0xFF - i);
      }
      final byte[] key = storedRangeKey(sortKey, new byte[] {(byte) 0xFF, 0x00, 0x00, 0x00});
      assertTrue("sort key length " + sortKeyLength, within(key, start, end));
    }
  }
}
