/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import com.google.common.primitives.UnsignedBytes;

public class FileSystemUtilsKeyEncodingTest {
  private static final int NUM_ITERATIONS = 10000;
  private static final int MAX_KEY_LENGTH = 24;
  private static final long SEED = 2894647323275155231L;

  @Test
  public void testFileNameRoundTrip() {
    final Random rand = new Random(SEED);
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      final byte[] key = randomKey(rand, MAX_KEY_LENGTH);
      final String fileName = FileSystemUtils.keyToFileName(key);
      assertTrue(fileName.endsWith(".bin"));
      assertArrayEquals(key, FileSystemUtils.fileNameToKey(fileName));
    }
  }

  /**
   * The bug this encoding exists to prevent: on a case-insensitive filesystem two file names that
   * differ only by case are the same file, so one key's entry silently overwrites another's.
   */
  @Test
  public void testDistinctKeysNeverCollideWhenCaseIsIgnored() {
    final Random rand = new Random(SEED);
    final Map<String, byte[]> namesIgnoringCase = new HashMap<>();
    for (int i = 0; i < 256; i++) {
      assertDistinctIgnoringCase(namesIgnoringCase, new byte[] {(byte) i});
    }
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      assertDistinctIgnoringCase(namesIgnoringCase, randomKey(rand, MAX_KEY_LENGTH));
    }
  }

  private static void assertDistinctIgnoringCase(
      final Map<String, byte[]> namesIgnoringCase,
      final byte[] key) {
    final String fileName = FileSystemUtils.keyToFileName(key);
    final byte[] previous =
        namesIgnoringCase.put(fileName.toLowerCase(Locale.ROOT), Arrays.copyOf(key, key.length));
    if ((previous != null) && !Arrays.equals(previous, key)) {
      throw new AssertionError(
          "'"
              + FileSystemUtils.keyToFileName(previous)
              + "' and '"
              + fileName
              + "' are the same file on a case-insensitive filesystem");
    }
  }

  /**
   * The read path treats a directory listing as a sorted key space -- {@code getSortedSet} takes a
   * {@code tailSet}/{@code headSet} over byte-range bounds, and metadata prefix scans run
   * {@code [prefix, nextPrefix)} -- so encoded names have to sort the way the raw keys do.
   */
  @Test
  public void testEncodingPreservesKeyOrder() {
    final Random rand = new Random(SEED);
    final List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keys.add(randomKey(rand, MAX_KEY_LENGTH));
    }
    // every prefix of a random key, so keys of differing length are compared against each other
    // and against the key they are a prefix of
    final byte[] base = randomKey(rand, MAX_KEY_LENGTH);
    for (int i = 0; i <= base.length; i++) {
      keys.add(Arrays.copyOf(base, i));
    }
    // runs of 0x00 and 0xff, the extremes of the unsigned byte ordering, at every length
    for (int i = 1; i <= MAX_KEY_LENGTH; i++) {
      final byte[] high = new byte[i];
      Arrays.fill(high, (byte) 0xff);
      keys.add(new byte[i]);
      keys.add(high);
    }

    final List<String> encoded = new ArrayList<>(keys.size());
    for (final byte[] key : keys) {
      encoded.add(FileSystemUtils.encodeKey(key));
    }
    for (int i = 0; i < keys.size(); i++) {
      for (int j = 0; j < keys.size(); j++) {
        assertEquals(
            "'" + encoded.get(i) + "' and '" + encoded.get(j) + "' do not sort like their keys",
            Integer.signum(
                UnsignedBytes.lexicographicalComparator().compare(keys.get(i), keys.get(j))),
            Integer.signum(encoded.get(i).compareTo(encoded.get(j))));
      }
    }
  }

  @Test
  public void testEmptyKey() {
    assertEquals(".bin", FileSystemUtils.keyToFileName(new byte[0]));
    assertArrayEquals(new byte[0], FileSystemUtils.fileNameToKey(".bin"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameFromAnEarlierVersionIsRejected() {
    // '_' is in the base64 alphabet earlier versions used, and in no base32hex name
    FileSystemUtils.fileNameToKey("a_b.bin");
  }

  private static byte[] randomKey(final Random rand, final int maxLength) {
    final byte[] key = new byte[rand.nextInt(maxLength) + 1];
    rand.nextBytes(key);
    return key;
  }
}
