/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.util.Arrays;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class ByteArrayUtilsTest {

  @Test
  public void testSplit() {
    final ByteArray first = new ByteArray("first");
    final ByteArray second = new ByteArray("second");
    final byte[] combined =
        ByteArrayUtils.combineVariableLengthArrays(first.getBytes(), second.getBytes());
    final Pair<byte[], byte[]> split = ByteArrayUtils.splitVariableLengthArrays(combined);
    Assert.assertArrayEquals(first.getBytes(), split.getLeft());
    Assert.assertArrayEquals(second.getBytes(), split.getRight());
  }

  @Test
  public void testVariableLengthEncodeDecode() {
    testVariableLengthValue(0);
    testVariableLengthValue(123456L);
    testVariableLengthValue(-42L);
    testVariableLengthValue(Byte.MAX_VALUE);
    testVariableLengthValue(Byte.MIN_VALUE);
    testVariableLengthValue(Integer.MIN_VALUE);
    testVariableLengthValue(Integer.MAX_VALUE);
    testVariableLengthValue(Long.MAX_VALUE);
    testVariableLengthValue(Long.MIN_VALUE);
  }

  @Test
  public void testCombineArrays() {
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3, 4}, ByteArrayUtils.combineArrays(new byte[] {1, 2, 3, 4}, null));
    Assert.assertArrayEquals(
        new byte[] {5, 6, 7, 8}, ByteArrayUtils.combineArrays(null, new byte[] {5, 6, 7, 8}));
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3, 4},
        ByteArrayUtils.combineArrays(new byte[] {1, 2, 3, 4}, new byte[] {}));
    Assert.assertArrayEquals(
        new byte[] {5, 6, 7, 8},
        ByteArrayUtils.combineArrays(new byte[] {}, new byte[] {5, 6, 7, 8}));
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
        ByteArrayUtils.combineArrays(new byte[] {1, 2, 3, 4}, new byte[] {5, 6, 7, 8}));
  }

  private void testVariableLengthValue(long value) {
    byte[] encoded = ByteArrayUtils.variableLengthEncode(value);
    long result = ByteArrayUtils.variableLengthDecode(encoded);
    Assert.assertEquals(value, result);
  }

  @Test
  public void testCompare() {
    Assert.assertEquals(-1, ByteArrayUtils.compare(new byte[] {}, null));
    Assert.assertEquals(0, ByteArrayUtils.compare(new byte[] {}, new byte[] {}));
    Assert.assertEquals(1, ByteArrayUtils.compare(new byte[] {0}, new byte[] {}));
    Assert.assertEquals(1, ByteArrayUtils.compare(new byte[] {1}, new byte[] {0}));
    Assert.assertEquals(0, ByteArrayUtils.compare(new byte[] {0}, new byte[] {0}));
    Assert.assertEquals(1, ByteArrayUtils.compare(null, new byte[] {0}));
    Assert.assertEquals(0, ByteArrayUtils.compare(null, null));
    Assert.assertEquals(0, ByteArrayUtils.compare(null, null));
  }

  @Test
  public void testByteArrayFromString() {
    Assert.assertArrayEquals(new byte[] {-75, -21, 45}, ByteArrayUtils.byteArrayFromString("test"));
    Assert.assertArrayEquals(new byte[] {}, ByteArrayUtils.byteArrayFromString(""));
  }

  @Test
  public void testIncrement() {
    byte[] testByte = new byte[] {-1};

    Assert.assertEquals(false, ByteArrayUtils.increment(testByte));
    Assert.assertArrayEquals(new byte[] {0}, testByte);

    Assert.assertEquals(true, ByteArrayUtils.increment(testByte));
    Assert.assertArrayEquals(new byte[] {1}, testByte);
  }

  @Test
  public void testByteArrayToShort() {
    Assert.assertEquals((short) 0, ByteArrayUtils.byteArrayToShort(new byte[] {0, 0}));
    Assert.assertEquals((short) 1, ByteArrayUtils.byteArrayToShort(new byte[] {1, 0}));
    Assert.assertEquals((short) 256, ByteArrayUtils.byteArrayToShort(new byte[] {0, 1}));
    Assert.assertEquals((short) 257, ByteArrayUtils.byteArrayToShort(new byte[] {1, 1}));
  }

  @Test
  public void testGetNextPrefix() {
    Assert.assertArrayEquals(
        new byte[] {-1, -1, -1, -1, -1, -1, -1}, ByteArrayUtils.getNextPrefix(new byte[] {}));
    Assert.assertArrayEquals(new byte[] {11}, ByteArrayUtils.getNextPrefix(new byte[] {10}));
    Assert.assertArrayEquals(
        new byte[] {-1, -1, -1, -1, -1, -1, -1, -1},
        ByteArrayUtils.getNextPrefix(new byte[] {(byte) 255}));
    Assert.assertArrayEquals(new byte[] {1}, ByteArrayUtils.getNextPrefix(new byte[] {(byte) 256}));
  }

  @Test
  public void testGetSingleRange() {
    Assert.assertNull(ByteArrayUtils.getSingleRange(null));
    ByteArrayRange firstRange =
        new ByteArrayRange(new byte[] {0, 0, 0, 1}, new byte[] {0, 0, 1, 0});
    ByteArrayRange secondRange =
        new ByteArrayRange(new byte[] {0, 0, 2, 0}, new byte[] {0, 1, 0, 0});
    ByteArrayRange singleRange =
        ByteArrayUtils.getSingleRange(Arrays.asList(firstRange, secondRange));
    Assert.assertArrayEquals(new byte[] {0, 0, 0, 1}, singleRange.getStart());
    Assert.assertArrayEquals(new byte[] {0, 1, 0, 0}, singleRange.getEnd());
  }

  @Test
  public void testShortToByteArray() {
    Assert.assertArrayEquals(new byte[] {0, 0}, ByteArrayUtils.shortToByteArray((short) 0));
    Assert.assertArrayEquals(new byte[] {1, 0}, ByteArrayUtils.shortToByteArray((short) 1));
    Assert.assertArrayEquals(new byte[] {0, 1}, ByteArrayUtils.shortToByteArray((short) 256));
    Assert.assertArrayEquals(new byte[] {1, 1}, ByteArrayUtils.shortToByteArray((short) 257));
  }

  @Test
  public void testCombineVariableLengthArrays() {
    Assert.assertArrayEquals(
        new byte[] {1, 1, 0, 0, 0, 1, 0, 0, 0, 1},
        ByteArrayUtils.combineVariableLengthArrays(new byte[] {1}, new byte[] {1}));
    Assert.assertArrayEquals(
        new byte[] {2, 3, 4, 0, 0, 0, 2, 0, 0, 0, 1},
        ByteArrayUtils.combineVariableLengthArrays(new byte[] {2, 3}, new byte[] {4}));
    Assert.assertArrayEquals(
        new byte[] {5, 6, 7, 0, 0, 0, 1, 0, 0, 0, 2},
        ByteArrayUtils.combineVariableLengthArrays(new byte[] {5}, new byte[] {6, 7}));
    Assert.assertArrayEquals(
        new byte[] {8, 9, 10, 11, 0, 0, 0, 2, 0, 0, 0, 2},
        ByteArrayUtils.combineVariableLengthArrays(new byte[] {8, 9}, new byte[] {10, 11}));
  }

  @Test
  public void testUuidToByteArray() {
    UUID uuid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d");
    Assert.assertArrayEquals(
        new byte[] {56, 64, 0, 0, -116, -16, 17, -67, -78, 62, 16, -71, 110, 78, -16, 13},
        ByteArrayUtils.uuidToByteArray(uuid));
  }

  @Test
  public void testLongToByteArray() {
    Assert.assertArrayEquals(
        new byte[] {18, 52, 86, 120, -112, -85, -51, -17},
        ByteArrayUtils.longToByteArray(0x1234567890ABCDEFl));
  }

  @Test
  public void testByteArrayToLong() {
    Assert.assertEquals(
        0x1234567890ABCDEFl,
        ByteArrayUtils.byteArrayToLong(new byte[] {18, 52, 86, 120, -112, -85, -51, -17}));
  }

  @Test
  public void testShortToString() {
    Assert.assertEquals("OgI", ByteArrayUtils.shortToString((short) 570));
  }

  @Test
  public void testShortFromString() {
    Assert.assertEquals((short) -5195, ByteArrayUtils.shortFromString("tes"));
  }

  @Test
  public void testGetHexString() {
    Assert.assertEquals(
        "FF 00 AB ",
        ByteArrayUtils.getHexString(new byte[] {(byte) 0xFF, (byte) 0x00, (byte) 0xAB}));
  }
}
