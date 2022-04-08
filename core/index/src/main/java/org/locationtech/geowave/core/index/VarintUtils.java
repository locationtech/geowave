/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import com.google.common.annotations.VisibleForTesting;

/**
 * Based on {@link com.clearspring.analytics.util.Varint}. Provides additional functionality to
 * encode varints directly to ByteBuffers.
 */
public class VarintUtils {
  @VisibleForTesting
  static long TIME_EPOCH = 1546300800000L; // Jan 1, 2019 UTC

  /**
   * Convert an int to an int that uses zig-zag encoding to prevent negative numbers from using the
   * maximum number of bytes.
   *
   * @see com.clearspring.analytics.util.Varint
   */
  @VisibleForTesting
  static int signedToUnsignedInt(final int value) {
    return (value << 1) ^ (value >> 31);
  }

  /**
   * Convert an int that has been zig-zag encoded back to normal.
   *
   * @see com.clearspring.analytics.util.Varint
   */
  @VisibleForTesting
  static int unsignedToSignedInt(final int value) {
    final int temp = (((value << 31) >> 31) ^ value) >> 1;
    return temp ^ (value & (1 << 31));
  }

  public static int signedIntByteLength(final int value) {
    return unsignedIntByteLength(signedToUnsignedInt(value));
  }

  public static int unsignedIntByteLength(final int value) {
    final int numRelevantBits = 32 - Integer.numberOfLeadingZeros(value);
    int numBytes = (numRelevantBits + 6) / 7;
    if (numBytes == 0) {
      numBytes = 1;
    }
    return numBytes;
  }

  public static int unsignedShortByteLength(final short value) {
    return unsignedIntByteLength(value & 0xFFFF);
  }

  public static void writeSignedInt(final int value, final ByteBuffer buffer) {
    writeUnsignedInt(signedToUnsignedInt(value), buffer);
  }

  public static byte[] writeSignedInt(final int value) {
    return writeUnsignedInt(signedToUnsignedInt(value));
  }

  public static void writeUnsignedInt(int value, final ByteBuffer buffer) {
    while ((value & 0xFFFFFF80) != 0) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) (value & 0x7F));
  }

  public static byte[] writeUnsignedInt(int value) {
    final byte[] retVal = new byte[unsignedIntByteLength(value)];
    int i = 0;
    while ((value & 0xFFFFFF80) != 0) {
      retVal[i++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    retVal[i] = (byte) (value & 0x7F);
    return retVal;
  }

  public static void writeUnsignedShort(final short value, final ByteBuffer buffer) {
    writeUnsignedInt(value & 0xFFFF, buffer);
  }

  public static byte[] writeUnsignedShort(final short value) {
    return writeUnsignedInt(value & 0xFFFF);
  }

  public static void writeUnsignedIntReversed(int value, final ByteBuffer buffer) {
    final int startPosition = buffer.position();
    final int byteLength = unsignedIntByteLength(value);
    int position = (startPosition + byteLength) - 1;
    while ((value & 0xFFFFFF80) != 0) {
      buffer.put(position, (byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      position--;
    }
    buffer.put(position, (byte) (value & 0x7F));
    buffer.position(startPosition + byteLength);
  }

  public static byte[] writeUnsignedIntReversed(int value) {
    final int byteLength = unsignedIntByteLength(value);
    final byte[] retVal = new byte[byteLength];
    int i = retVal.length - 1;
    while ((value & 0xFFFFFF80) != 0) {
      retVal[i--] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    retVal[0] = (byte) (value & 0x7F);
    return retVal;
  }

  public static int readSignedInt(final ByteBuffer buffer) {
    return unsignedToSignedInt(readUnsignedInt(buffer));
  }

  public static int readUnsignedInt(final ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    int currByte;
    while (((currByte = buffer.get()) & 0x80) != 0) {
      value |= (currByte & 0x7F) << i;
      i += 7;
    }
    return value | (currByte << i);
  }

  public static short readUnsignedShort(final ByteBuffer buffer) {
    final int value = readUnsignedInt(buffer);
    return (short) (value & 0xFFFF);
  }

  public static int readUnsignedIntReversed(final ByteBuffer buffer) {
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
   * @see com.clearspring.analytics.util.Varint
   */
  @VisibleForTesting
  static long signedToUnsignedLong(final long value) {
    return (value << 1) ^ (value >> 63);
  }

  /**
   * Convert a long that has been zig-zag encoded back to normal.
   *
   * @see com.clearspring.analytics.util.Varint
   */
  @VisibleForTesting
  static long unsignedToSignedLong(final long value) {
    final long temp = (((value << 63) >> 63) ^ value) >> 1;
    return temp ^ (value & (1L << 63));
  }

  public static int signedLongByteLength(final long value) {
    return unsignedLongByteLength(signedToUnsignedLong(value));
  }

  public static int unsignedLongByteLength(final long value) {
    final int numRelevantBits = 64 - Long.numberOfLeadingZeros(value);
    int numBytes = (numRelevantBits + 6) / 7;
    if (numBytes == 0) {
      numBytes = 1;
    }
    return numBytes;
  }

  public static void writeSignedLong(final long value, final ByteBuffer buffer) {
    writeUnsignedLong(signedToUnsignedLong(value), buffer);
  }

  public static byte[] writeSignedLong(final long value) {
    return writeUnsignedLong(signedToUnsignedLong(value));
  }

  public static void writeUnsignedLong(long value, final ByteBuffer buffer) {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) (value & 0x7F));
  }

  public static byte[] writeUnsignedLong(long value) {
    final byte[] retVal = new byte[unsignedLongByteLength(value)];
    int i = 0;
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      retVal[i++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    retVal[i] = (byte) (value & 0x7F);
    return retVal;
  }

  public static long readSignedLong(final ByteBuffer buffer) {
    return unsignedToSignedLong(readUnsignedLong(buffer));
  }

  public static long readUnsignedLong(final ByteBuffer buffer) {
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
  public static int timeByteLength(final long time) {
    return signedLongByteLength(time - TIME_EPOCH);
  }

  /**
   * Encode a timestamp using varint encoding.
   *
   * @param time the timestamp
   * @param buffer the {@code ByteBuffer} to write the timestamp to
   */
  public static void writeTime(final long time, final ByteBuffer buffer) {
    writeSignedLong(time - TIME_EPOCH, buffer);
  }

  /**
   * Encode a timestamp using varint encoding.
   *
   * @param time the timestamp
   * @return the timestamp as bytes
   */
  public static byte[] writeTime(final long time) {
    return writeSignedLong(time - TIME_EPOCH);
  }

  /**
   * Read a timestamp from a {@code ByteBuffer} that was previously encoded with {@link #writeTime}.
   *
   * @param buffer the {@code ByteBuffer} to read from
   * @return the decoded timestamp
   */
  public static long readTime(final ByteBuffer buffer) {
    return VarintUtils.readSignedLong(buffer) + TIME_EPOCH;
  }

  /**
   * Encode a BigDecimal as a byte[]. The structure of the byte[] is opaque, so to deserialize, use
   * {@link #readBigDecimal(ByteBuffer)}
   *
   * @param num The number to serialize as a {@link ByteBuffer}
   * @return a byte array that represents the given BigDecimal.
   */
  public static byte[] writeBigDecimal(final BigDecimal num) {
    if (num == null) {
      return new byte[0];
    }
    final byte[] unscaled = num.unscaledValue().toByteArray();

    final ByteBuffer buf =
        ByteBuffer.allocate(VarintUtils.signedIntByteLength(num.scale()) + 4 + unscaled.length);
    VarintUtils.writeSignedInt(num.scale(), buf);
    buf.putInt(unscaled.length);
    buf.put(unscaled);
    return buf.array();
  }

  /**
   * Read a BigDecimal number from a {@link ByteBuffer} that was previously encoded by using
   * {@link #writeBigDecimal(BigDecimal)}
   *
   * @param buffer The {@link ByteBuffer} that contains the BigDecimal next in its contents.
   * @return The BigDecimal that was stored in the ByteBuffer, and the ByteBuffer's position is
   *         modified past the BigDecimal.
   */
  public static BigDecimal readBigDecimal(final ByteBuffer buffer) {
    if (buffer.remaining() == 0) {
      return null;
    }
    final int scale = VarintUtils.readSignedInt(buffer);
    final int unscaledLength = buffer.getInt();
    final byte[] unscaled = new byte[unscaledLength];
    buffer.get(unscaled);
    return new BigDecimal(new BigInteger(unscaled), scale);
  }
}
