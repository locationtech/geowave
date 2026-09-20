/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.List;
import org.junit.Test;
import com.google.common.collect.Lists;

public class DynamoDBUtilsTest {
  @Test
  public void testSortableBase64EncodeDecode() {
    final String testString = new String("Test converting to and from sortable base64.");
    final byte[] testValue = testString.getBytes();
    final byte[] encoded = DynamoDBUtils.encodeSortableBase64(testValue);
    assertTrue(encoded.length > testValue.length);
    final byte[] decoded = DynamoDBUtils.decodeSortableBase64(encoded);
    final String decodedString = new String(decoded);
    assertTrue(testString.equals(decodedString));
  }

  @Test
  public void testSortableBase64Sorting() {
    final List<byte[]> sortedBinary = Lists.newArrayList();
    for (int i = 0; i < Short.MAX_VALUE; i += 100) {
      final byte[] bytes = new byte[2];
      bytes[1] = (byte) (i & 0xff);
      bytes[0] = (byte) ((i >> 8) & 0xff);
      sortedBinary.add(bytes);
    }
    for (int i = Short.MIN_VALUE; i < 0; i += 100) {
      final byte[] bytes = new byte[2];
      bytes[1] = (byte) (i & 0xff);
      bytes[0] = (byte) ((i >> 8) & 0xff);
      sortedBinary.add(bytes);
    }
    verifySorted(sortedBinary);
    final List<byte[]> encodedBinary =
        Lists.transform(sortedBinary, (binary) -> DynamoDBUtils.encodeSortableBase64(binary));
    verifySorted(encodedBinary);
  }

  /**
   * The encoding preserves order only between keys of the same length. Base64 pads with '=', which
   * this alphabet maps to itself at 0x3D -- between '9' (0x39) and 'A' (0x41) rather than below
   * everything -- so a padded short key can sort above a longer one that is smaller in raw form.
   *
   * <p> The text index stores mixed lengths in one partition and is affected.
   * {@code TextIndexUtils.getForwardInsertionIds} uses a constant partition key and the raw text
   * bytes as the sort key, so one partition holds every indexed string at whatever length it
   * happens to be. Prefix searches against it lose rows at both ends of the range: a stored key
   * that extends the query's start can encode below it, and one that extends the query's end can
   * encode above it.
   *
   * <p> Tiered spatial indexes are not affected, because
   * {@code BinnedSFCUtils.getSingleBinnedInsertionId} puts the tier byte in the <em>partition</em>
   * key, so every sort key in one of those partitions comes from a single space filling curve at a
   * single length.
   *
   * <p> Fixing this means an encoding that orders across lengths, which changes the stored key
   * format and needs a migration. Until then this records the defect rather than hiding it.
   */
  @Test
  public void encodingIsOrderPreservingOnlyWithinALength() {
    final byte[] shorter = new byte[] {(byte) 0x97};
    final byte[] longer =
        new byte[] {(byte) 0x97, 0x01, (byte) 0xcc, (byte) 0xc5, 0x52, (byte) 0xc4, 0x06};
    assertTrue("raw: the longer key is greater", compareUnsigned(shorter, longer) < 0);
    assertTrue(
        "encoded: the padded shorter key sorts above it, which is the whole caveat",
        compareUnsigned(
            DynamoDBUtils.encodeSortableBase64(shorter),
            DynamoDBUtils.encodeSortableBase64(longer)) > 0);
  }

  private static int compareUnsigned(final byte[] a, final byte[] b) {
    for (int i = 0; (i < a.length) && (i < b.length); i++) {
      final int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
      if (diff != 0) {
        return diff;
      }
    }
    return a.length - b.length;
  }

  private void verifySorted(final List<byte[]> list) {
    byte[] last = null;
    for (final byte[] binary : list) {
      if (last != null) {
        boolean less = false;
        for (int i = 0; (i < last.length) & (i < binary.length); i++) {
          if ((binary[i] & 0xFF) < (last[i] & 0xFF)) {
            fail();
          } else if ((binary[i] & 0xFF) > (last[i] & 0xFF)) {
            less = true;
            break;
          }
        }
        if (!less && (binary.length > last.length)) {
          fail();
        }
      }
      last = binary;
    }
  }
}
