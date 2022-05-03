/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

/**
 * Convenience methods for converting binary data to and from strings. The encoding and decoding is
 * done in base-64. These methods should be used for converting data that is binary in nature to a
 * String representation for transport. Use StringUtils for serializing and deserializing text-based
 * data.
 *
 * <p> Additionally, this class has methods for manipulating byte arrays, such as combining or
 * incrementing them.
 */
public class ByteArrayUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayUtils.class);
  private static Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

  private static byte[] internalCombineArrays(final byte[] beginning, final byte[] end) {
    final byte[] combined = new byte[beginning.length + end.length];
    System.arraycopy(beginning, 0, combined, 0, beginning.length);
    System.arraycopy(end, 0, combined, beginning.length, end.length);
    return combined;
  }

  /**
   * Convert binary data to a string for transport
   *
   * @param byteArray the binary data
   * @return the base64url encoded string
   */
  public static String byteArrayToString(final byte[] byteArray) {
    return new String(ENCODER.encode(byteArray), StringUtils.getGeoWaveCharset());
  }

  /**
   * Convert a string representation of binary data back to a String
   *
   * @param str the string representation of binary data
   * @return the base64url decoded binary data
   */
  public static byte[] byteArrayFromString(final String str) {
    return Base64.getUrlDecoder().decode(str);
  }

  /**
   * Throw an exception if the requested length is longer than the remaining buffer size.
   *
   * @param buffer the byte buffer
   * @param length the number of bytes to read
   */
  public static void verifyBufferSize(final ByteBuffer buffer, final int length) {
    if (length > buffer.remaining()) {
      throw new GeoWaveSerializationException(
          "Tried to read more data than was available in buffer.");
    }
  }

  /**
   * Read bytes from the buffer, but only if the buffer's remaining length supports it.
   *
   * @param buffer the byte buffer
   * @param length the number of bytes to read
   * @return the bytes that were read
   */
  public static byte[] safeRead(final ByteBuffer buffer, final int length) {
    verifyBufferSize(buffer, length);
    final byte[] readBytes = new byte[length];
    if (length > 0) {
      buffer.get(readBytes);
    }
    return readBytes;
  }

  /**
   * Combine 2 arrays into one large array. If both are not null it will append id2 to id1 and the
   * result will be of length id1.length + id2.length
   *
   * @param id1 the first byte array to use (the start of the result)
   * @param id2 the second byte array to combine (appended to id1)
   * @return the concatenated byte array
   */
  public static byte[] combineArrays(final byte[] id1, final byte[] id2) {
    byte[] combinedId;
    if ((id1 == null) || (id1.length == 0)) {
      combinedId = id2;
    } else if ((id2 == null) || (id2.length == 0)) {
      combinedId = id1;
    } else {
      // concatenate bin ID 2 to the end of bin ID 1
      combinedId = ByteArrayUtils.internalCombineArrays(id1, id2);
    }
    return combinedId;
  }

  public static byte[] replace(final byte[] arr, final byte[] find, final byte[] replace) {
    if ((find == null) || (find.length == 0) || (find.length > arr.length) || (replace == null)) {
      return arr;
    }
    int match = 0;
    int matchCount = 0;
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] == find[match]) {
        match++;
        if (match == find.length) {
          matchCount++;
          match = 0;
        }
      } else if ((match > 0) && (arr[i] == find[0])) {
        match = 1;
      } else {
        match = 0;
      }
    }
    if (matchCount == 0) {
      return arr;
    }
    final byte[] newBytes =
        new byte[(arr.length - (find.length * matchCount)) + (replace.length * matchCount)];
    match = 0;
    int copyIdx = 0;
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] == find[match]) {
        match++;
        if (match == find.length) {
          for (int j = 0; j < replace.length; j++) {
            newBytes[copyIdx++] = replace[j];
          }
          match = 0;
        }
        continue;
      } else if (match > 0) {
        for (int j = i - match; j < i; j++) {
          newBytes[copyIdx++] = arr[j];
        }
        if (arr[i] == find[0]) {
          copyIdx--;
          match = 1;
        } else {
          match = 0;
        }
      }
      if (match == 0) {
        newBytes[copyIdx++] = arr[i];
      }
    }
    return newBytes;
  }

  /**
   * add 1 to the least significant bit in this byte array (the last byte in the array)
   *
   * @param value the array to increment
   * @return will return true as long as the value did not overflow
   */
  public static boolean increment(final byte[] value) {
    for (int i = value.length - 1; i >= 0; i--) {
      value[i]++;
      if (value[i] != 0) {
        return true;
      }
    }
    return value[0] != 0;
  }

  /**
   * Converts a UUID to a byte array
   *
   * @param uuid the uuid
   * @return the byte array representing that UUID
   */
  public static byte[] uuidToByteArray(final UUID uuid) {
    final ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  /**
   * Converts a long to a byte array
   *
   * @param l the long
   * @return the byte array representing that long
   */
  public static byte[] longToByteArray(final long l) {
    final ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
    bb.putLong(l);
    return bb.array();
  }

  /**
   * Converts a byte array to a long
   *
   * @param bytes the byte array the long
   * @return the long represented by the byte array
   */
  public static long byteArrayToLong(final byte[] bytes) {
    final ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
    bb.put(bytes);
    bb.flip();
    return bb.getLong();
  }


  public static byte[] longToBytes(long val) {
    final int radix = 1 << 8;
    final int mask = radix - 1;
    // we want to eliminate trailing 0's (ie. truncate the byte array by
    // trailing 0's)
    int trailingZeros = 0;
    while ((((int) val) & mask) == 0) {
      val >>>= 8;
      trailingZeros++;
      if (trailingZeros == 8) {
        return new byte[0];
      }
    }
    final byte[] array = new byte[8 - trailingZeros];
    int pos = array.length;
    do {
      array[--pos] = (byte) (((int) val) & mask);
      val >>>= 8;

    } while ((val != 0) && (pos > 0));

    return array;
  }

  public static long bytesToLong(final byte[] bytes) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value = (value << 8);
      if (i < bytes.length) {
        value += (bytes[i] & 0xff);
      }
    }
    return value;
  }

  /**
   * Combines two variable length byte arrays into one large byte array and appends the length of
   * each individual byte array in sequential order at the end of the combined byte array.
   *
   * <p> Given byte_array_1 of length 8 + byte_array_2 of length 16, the result will be byte_array1
   * + byte_array_2 + 8 + 16.
   *
   * <p> Lengths are put after the individual arrays so they don't impact sorting when used within
   * the key of a sorted key-value data store.
   *
   * @param array1 the first byte array
   * @param array2 the second byte array
   * @return the combined byte array including the individual byte array lengths
   */
  public static byte[] combineVariableLengthArrays(final byte[] array1, final byte[] array2) {
    Preconditions.checkNotNull(array1, "First byte array cannot be null");
    Preconditions.checkNotNull(array2, "Second byte array cannot be null");
    Preconditions.checkArgument(array1.length > 1, "First byte array cannot have length 0");
    Preconditions.checkArgument(array2.length > 1, "Second byte array cannot have length 0");
    final byte[] combinedWithoutLengths = ByteArrayUtils.internalCombineArrays(array1, array2);
    final ByteBuffer combinedWithLengthsAppended =
        ByteBuffer.allocate(combinedWithoutLengths.length + 8); // 8
    // for
    // two
    // integer
    // lengths
    combinedWithLengthsAppended.put(combinedWithoutLengths);
    combinedWithLengthsAppended.putInt(array1.length);
    combinedWithLengthsAppended.putInt(array2.length);
    return combinedWithLengthsAppended.array();
  }

  public static Pair<byte[], byte[]> splitVariableLengthArrays(final byte[] combinedArray) {
    final ByteBuffer combined = ByteBuffer.wrap(combinedArray);
    final byte[] combinedArrays = new byte[combinedArray.length - 8];
    combined.get(combinedArrays);
    final ByteBuffer bb = ByteBuffer.wrap(combinedArrays);
    final int len1 = combined.getInt();
    final int len2 = combined.getInt();
    final byte[] part1 = new byte[len1];
    final byte[] part2 = new byte[len2];
    bb.get(part1);
    bb.get(part2);
    return Pair.of(part1, part2);
  }

  public static String shortToString(final short input) {
    return byteArrayToString(shortToByteArray(input));
  }

  public static short shortFromString(final String input) {
    return byteArrayToShort(byteArrayFromString(input));
  }

  public static byte[] shortToByteArray(final short input) {
    return new byte[] {(byte) (input & 0xFF), (byte) ((input >> 8) & 0xFF)};
  }

  public static short byteArrayToShort(final byte[] bytes) {
    int r = bytes[1] & 0xFF;
    r = (r << 8) | (bytes[0] & 0xFF);
    return (short) r;
  }

  public static byte[] variableLengthEncode(long n) {
    final int numRelevantBits = 64 - Long.numberOfLeadingZeros(n);
    int numBytes = (numRelevantBits + 6) / 7;
    if (numBytes == 0) {
      numBytes = 1;
    }
    final byte[] output = new byte[numBytes];
    for (int i = numBytes - 1; i >= 0; i--) {
      int curByte = (int) (n & 0x7F);
      if (i != (numBytes - 1)) {
        curByte |= 0x80;
      }
      output[i] = (byte) curByte;
      n >>>= 7;
    }
    return output;
  }

  public static long variableLengthDecode(final byte[] b) {
    long n = 0;
    for (int i = 0; i < b.length; i++) {
      final int curByte = b[i] & 0xFF;
      n = (n << 7) | (curByte & 0x7F);
      if ((curByte & 0x80) == 0) {
        break;
      }
    }
    return n;
  }

  public static byte[] getNextPrefix(final byte[] rowKeyPrefix) {
    int offset = rowKeyPrefix.length;
    while (offset > 0) {
      if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      return getNextInclusive(rowKeyPrefix);
    }

    final byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
    // And increment the last one
    newStopRow[newStopRow.length - 1]++;
    return newStopRow;
  }

  public static byte[] getNextInclusive(final byte[] rowKeyPrefix) {
    return ByteArrayUtils.combineArrays(
        rowKeyPrefix,
        new byte[] {
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF,
            (byte) 0xFF});
  }

  public static byte[] getPreviousPrefix(final byte[] rowKeyPrefix) {
    int offset = rowKeyPrefix.length;
    while (offset > 0) {
      if (rowKeyPrefix[offset - 1] != (byte) 0x00) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      return new byte[0];
    }

    final byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
    // And decrement the last one
    newStopRow[newStopRow.length - 1]--;
    return newStopRow;
  }

  public static int compare(final byte[] array1, final byte[] array2) {
    if (array2 == null) {
      if (array1 == null) {
        return 0;
      }
      return -1;
    }
    if (array1 == null) {
      return 1;
    }
    for (int i = 0, j = 0; (i < array1.length) && (j < array2.length); i++, j++) {
      final int a = (array1[i] & 0xff);
      final int b = (array2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return array1.length - array2.length;
  }

  public static int compareToPrefix(final byte[] array, final byte[] prefix) {
    if (prefix == null) {
      if (array == null) {
        return 0;
      }
      return -1;
    }
    if (array == null) {
      return 1;
    }
    for (int i = 0, j = 0; (i < array.length) && (j < prefix.length); i++, j++) {
      final int a = (array[i] & 0xff);
      final int b = (prefix[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    if (prefix.length <= array.length) {
      return 0;
    }
    for (int i = array.length; i < prefix.length; i++) {
      final int a = (prefix[i] & 0xff);
      if (a != 0) {
        return -1;
      }
    }
    return 0;
  }

  public static boolean startsWith(final byte[] bytes, final byte[] prefix) {
    if ((bytes == null) || (prefix == null) || (prefix.length > bytes.length)) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (bytes[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  public static boolean endsWith(final byte[] bytes, final byte[] suffix) {
    if ((bytes == null) || (suffix == null) || (suffix.length > bytes.length)) {
      return false;
    }
    final int suffixEnd = suffix.length - 1;
    final int bytesEnd = bytes.length - 1;
    for (int i = 0; i < suffix.length; i++) {
      if (bytes[bytesEnd - i] != suffix[suffixEnd - i]) {
        return false;
      }
    }
    return true;
  }

  public static boolean matchesPrefixRanges(final byte[] bytes, final List<ByteArrayRange> ranges) {
    return ranges.stream().anyMatch(range -> {
      return (ByteArrayUtils.compareToPrefix(bytes, range.getStart()) >= 0)
          && (ByteArrayUtils.compareToPrefix(bytes, range.getEnd()) <= 0);
    });
  }

  public static String getHexString(final byte[] bytes) {
    final StringBuffer str = new StringBuffer();
    for (final byte b : bytes) {
      str.append(String.format("%02X ", b));
    }
    return str.toString();
  }

  public static ByteArrayRange getSingleRange(final List<ByteArrayRange> ranges) {
    byte[] start = null;
    byte[] end = null;
    if (ranges == null) {
      return null;
    }
    for (final ByteArrayRange range : ranges) {
      if ((start == null) || (ByteArrayUtils.compare(range.getStart(), start) < 0)) {
        start = range.getStart();
      }
      if ((end == null) || (ByteArrayUtils.compare(range.getEnd(), end) > 0)) {
        end = range.getEnd();
      }
    }
    return new ByteArrayRange(start, end);
  }

  public static void addAllIntermediaryByteArrays(
      final List<byte[]> retVal,
      final ByteArrayRange range) {
    byte[] start;
    byte[] end;
    // they had better not both be null or this method would quickly eat up memory
    if (range.getStart() == null) {
      start = new byte[0];
    } else {
      start = range.getStart();
    }
    if (range.getEnd() == null) {
      // this isn't precisely the end because the actual end is infinite, it'd be far better to set
      // the start and end but this at least covers the edge case if they're not given
      end =
          new byte[] {
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF,
              (byte) 0xFF};
    } else {
      end = range.getEnd();
    }
    byte[] currentRowId = Arrays.copyOf(start, start.length);
    retVal.add(currentRowId);
    while (!Arrays.equals(currentRowId, end)) {
      currentRowId = Arrays.copyOf(currentRowId, currentRowId.length);
      // increment until we reach the end row ID
      boolean overflow = !ByteArrayUtils.increment(currentRowId);
      if (!overflow) {
        retVal.add(currentRowId);
      } else {
        // the increment caused an overflow which shouldn't
        // ever happen assuming the start row ID is less
        // than the end row ID
        LOGGER.warn(
            "Row IDs overflowed when ingesting data; start of range decomposition must be less than or equal to end of range. This may be because the start of the decomposed range is higher than the end of the range.");
        overflow = true;
        break;
      }
    }
  }
}
