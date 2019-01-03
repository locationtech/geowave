/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;

/**
 * Based on {@link com.clearspring.analytics.util.Varint}. Provides additional functionality to
 * encode varints directly to ByteBuffers.
 */
public class VarintUtils {
  @VisibleForTesting static long TIME_EPOCH = 1546300800000L; // Jan 1, 2019 UTC

  /**
   * Convert an int to an int that uses zig-zag encoding to prevent negative numbers from using the
   * maximum number of bytes.
   *
   * @see {@code com.clearsprint.analytics.util.Varint}
   */
  @VisibleForTesting
  static int signedToUnsignedInt(int value) {
    return (value << 1) ^ (value >> 31);
  }

  /**
   * Convert an int that has been zig-zag encoded back to normal.
   *
   * @see {@code com.clearsprint.analytics.util.Varint}
   */
  @VisibleForTesting
  static int unsignedToSignedInt(int value) {
    int temp = (((value << 31) >> 31) ^ value) >> 1;
    return temp ^ (value & (1 << 31));
  }

  public static int signedIntByteLength(int value) {
    return unsignedIntByteLength(signedToUnsignedInt(value));
  }

  public static int unsignedIntByteLength(int value) {
    final int numRelevantBits = 32 - Integer.numberOfLeadingZeros(value);
    int numBytes = (numRelevantBits + 6) / 7;
    if (numBytes == 0) {
      numBytes = 1;
    }
    return numBytes;
  }

  public static int unsignedShortByteLength(short value) {
    return unsignedIntByteLength(value & 0xFFFF);
  }

  public static void writeSignedInt(int value, ByteBuffer buffer) {
    writeUnsignedInt(signedToUnsignedInt(value), buffer);
  }

  public static void writeUnsignedInt(int value, ByteBuffer buffer) {
    while ((value & 0xFFFFFF80) != 0) {
      buffer.put((byte) (value & 0x7F | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) (value & 0x7F));
  }

  public static void writeUnsignedShort(short value, ByteBuffer buffer) {
    writeUnsignedInt(value & 0xFFFF, buffer);
  }

  public static void writeUnsignedIntReversed(int value, ByteBuffer buffer) {
    int startPosition = buffer.position();
    int byteLength = VarintUtils.unsignedIntByteLength(value);
    int position = startPosition + byteLength - 1;
    while ((value & 0xFFFFFF80) != 0) {
      buffer.put(position, (byte) (value & 0x7F | 0x80));
      value >>>= 7;
      position--;
    }
    buffer.put(position, (byte) (value & 0x7F));
    buffer.position(startPosition + byteLength);
  }

  public static int readSignedInt(ByteBuffer buffer) {
    return unsignedToSignedInt(readUnsignedInt(buffer));
  }

  public static int readUnsignedInt(ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    int currByte;
    while (((currByte = buffer.get()) & 0x80) != 0) {
      value |= (currByte & 0x7F) << i;
      i += 7;
    }
    return value | (currByte << i);
  }

  public static short readUnsignedShort(ByteBuffer buffer) {
    int value = readUnsignedInt(buffer);
    return (short) (value & 0xFFFF);
  }

  public static int readUnsignedIntReversed(ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    int currByte;
    int position = buffer.position();
    while (((currByte = buffer.get(position)) & 0x80) != 0) {
      value |= (currByte & 0x7F) << i;
      i += 7;
      position--;
    }
    if (position > 0) {
      buffer.position(position - 1);
    }
    return value | (currByte << i);
  }

  /**
   * Convert a long to a long that uses zig-zag encoding to prevent negative numbers from using the
   * maximum number of bytes.
   *
   * @see {@code com.clearsprint.analytics.util.Varint}
   */
  @VisibleForTesting
  static long signedToUnsignedLong(long value) {
    return (value << 1) ^ (value >> 63);
  }

  /**
   * Convert a long that has been zig-zag encoded back to normal.
   *
   * @see {@code com.clearsprint.analytics.util.Varint}
   */
  @VisibleForTesting
  static long unsignedToSignedLong(long value) {
    long temp = (((value << 63) >> 63) ^ value) >> 1;
    return temp ^ (value & (1L << 63));
  }

  public static int signedLongByteLength(long value) {
    return unsignedLongByteLength(signedToUnsignedLong(value));
  }

  public static int unsignedLongByteLength(long value) {
    final int numRelevantBits = 64 - Long.numberOfLeadingZeros(value);
    int numBytes = (numRelevantBits + 6) / 7;
    if (numBytes == 0) {
      numBytes = 1;
    }
    return numBytes;
  }

  public static void writeSignedLong(long value, ByteBuffer buffer) {
    writeUnsignedLong(signedToUnsignedLong(value), buffer);
  }

  public static void writeUnsignedLong(long value, ByteBuffer buffer) {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buffer.put((byte) (value & 0x7F | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) (value & 0x7F));
  }

  public static long readSignedLong(ByteBuffer buffer) {
    return unsignedToSignedLong(readUnsignedLong(buffer));
  }

  public static long readUnsignedLong(ByteBuffer buffer) {
    long value = 0;
    int i = 0;
    long currByte;
    while (((currByte = buffer.get()) & 0x80L) != 0) {
      value |= (currByte & 0x7F) << i;
      i += 7;
    }
    return value | (currByte << i);
  }

  /**
   * Get the byte length of a varint encoded timestamp.
   *
   * @param time the timestamp
   * @return the number of bytes the encoded timestamp will use
   */
  public static int timeByteLength(long time) {
    return VarintUtils.signedLongByteLength(time - TIME_EPOCH);
  }

  /**
   * Encode a timestamp using varint encoding.
   *
   * @param time the timestamp
   * @param buffer the {@code ByteBuffer} to write the timestamp to
   */
  public static void writeTime(long time, ByteBuffer buffer) {
    VarintUtils.writeSignedLong(time - TIME_EPOCH, buffer);
  }

  /**
   * Read a timestamp from a {@code ByteBuffer} that was previously encoded with {@link #writeTime}.
   *
   * @param buffer the {@code ByteBuffer} to read from
   * @return the decoded timestamp
   */
  public static long readTime(ByteBuffer buffer) {
    return VarintUtils.readSignedLong(buffer) + TIME_EPOCH;
  }
}
