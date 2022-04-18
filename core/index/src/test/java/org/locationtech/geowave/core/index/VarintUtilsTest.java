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
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class VarintUtilsTest {

  @Test
  public void testVarintEncodeDecodeUnsignedIntReversed() {
    testEncodeDecodeUnsignedIntReversed(0);
    testEncodeDecodeUnsignedIntReversed(123456);
    testEncodeDecodeUnsignedIntReversed(Byte.MAX_VALUE);
    testEncodeDecodeUnsignedIntReversed(Integer.MAX_VALUE);

    final int length =
        VarintUtils.unsignedIntByteLength(15) + VarintUtils.unsignedIntByteLength(Byte.MAX_VALUE);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeUnsignedIntReversed(15, buffer);
    VarintUtils.writeUnsignedIntReversed(Byte.MAX_VALUE, buffer);
    buffer.position(buffer.limit() - 1);
    Assert.assertEquals(Byte.MAX_VALUE, VarintUtils.readUnsignedIntReversed(buffer));
    Assert.assertEquals(15, VarintUtils.readUnsignedIntReversed(buffer));
  }

  private void testEncodeDecodeUnsignedIntReversed(final int value) {
    final int length = VarintUtils.unsignedIntByteLength(value);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeUnsignedIntReversed(value, buffer);
    buffer.position(buffer.limit() - 1);
    final int decoded = VarintUtils.readUnsignedIntReversed(buffer);
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testVarintSignedUnsignedInt() {
    testSignedUnsignedIntValue(0);
    testSignedUnsignedIntValue(-123456);
    testSignedUnsignedIntValue(123456);
    testSignedUnsignedIntValue(Byte.MIN_VALUE);
    testSignedUnsignedIntValue(Byte.MAX_VALUE);
    testSignedUnsignedIntValue(Integer.MIN_VALUE);
    testSignedUnsignedIntValue(Integer.MAX_VALUE);
  }

  private void testSignedUnsignedIntValue(final int value) {
    final int unsigned = VarintUtils.signedToUnsignedInt(value);
    final int signed = VarintUtils.unsignedToSignedInt(unsigned);
    Assert.assertEquals(value, signed);
  }

  @Test
  public void testVarintEncodeDecodeUnsignedInt() {
    testEncodeDecodeUnsignedIntValue(0);
    testEncodeDecodeUnsignedIntValue(123456);
    testEncodeDecodeUnsignedIntValue(Byte.MAX_VALUE);
    testEncodeDecodeUnsignedIntValue(Integer.MAX_VALUE);

    // negative values are inefficient in this encoding, but should still
    // work.
    testEncodeDecodeUnsignedIntValue(-123456);
    testEncodeDecodeUnsignedIntValue(Byte.MIN_VALUE);
    testEncodeDecodeUnsignedIntValue(Integer.MIN_VALUE);
  }

  private void testEncodeDecodeUnsignedIntValue(final int value) {
    final int length = VarintUtils.unsignedIntByteLength(value);
    // should never use more than 5 bytes
    Assert.assertTrue(length <= 5);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeUnsignedInt(value, buffer);
    buffer.position(0);
    final int decoded = VarintUtils.readUnsignedInt(buffer);
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testVarintEncodeDecodeUnsignedShort() {
    testEncodeDecodeUnsignedShortValue((short) 0);
    testEncodeDecodeUnsignedShortValue((short) 12345);
    testEncodeDecodeUnsignedShortValue(Byte.MAX_VALUE);
    testEncodeDecodeUnsignedShortValue(Short.MAX_VALUE);

    // negative values are inefficient in this encoding, but should still
    // work.
    testEncodeDecodeUnsignedShortValue((short) -12345);
    testEncodeDecodeUnsignedShortValue(Byte.MIN_VALUE);
    testEncodeDecodeUnsignedShortValue(Short.MIN_VALUE);
  }

  private void testEncodeDecodeUnsignedShortValue(final short value) {
    final int length = VarintUtils.unsignedShortByteLength(value);
    // should never use more than 3 bytes
    Assert.assertTrue(length <= 3);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeUnsignedShort(value, buffer);
    buffer.position(0);
    final int decoded = VarintUtils.readUnsignedShort(buffer);
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testVarintEncodeDecodeSignedInt() {
    testEncodeDecodeSignedIntValue(0);
    testEncodeDecodeSignedIntValue(-123456);
    testEncodeDecodeSignedIntValue(123456);
    testEncodeDecodeSignedIntValue(Byte.MIN_VALUE);
    testEncodeDecodeSignedIntValue(Byte.MAX_VALUE);
    testEncodeDecodeSignedIntValue(Integer.MIN_VALUE);
    testEncodeDecodeSignedIntValue(Integer.MAX_VALUE);
  }

  private void testEncodeDecodeSignedIntValue(final int value) {
    final int length = VarintUtils.signedIntByteLength(value);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeSignedInt(value, buffer);
    buffer.position(0);
    final int decoded = VarintUtils.readSignedInt(buffer);
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testVarintSignedUnsignedLong() {
    testSignedUnsignedLongValue(0L);
    testSignedUnsignedLongValue(-123456L);
    testSignedUnsignedLongValue(123456L);
    testSignedUnsignedLongValue(Byte.MIN_VALUE);
    testSignedUnsignedLongValue(Byte.MAX_VALUE);
    testSignedUnsignedLongValue(Integer.MIN_VALUE);
    testSignedUnsignedLongValue(Integer.MAX_VALUE);
    testSignedUnsignedLongValue(Long.MIN_VALUE);
    testSignedUnsignedLongValue(Long.MAX_VALUE);
  }

  private void testSignedUnsignedLongValue(final long value) {
    final long unsigned = VarintUtils.signedToUnsignedLong(value);
    final long signed = VarintUtils.unsignedToSignedLong(unsigned);
    Assert.assertEquals(value, signed);
  }

  @Test
  public void testVarLongEncodeDecodeUnsignedLong() {
    testEncodeDecodeUnsignedLongValue(0L);
    testEncodeDecodeUnsignedLongValue(123456L);
    testEncodeDecodeUnsignedLongValue(Byte.MAX_VALUE);
    testEncodeDecodeUnsignedLongValue(Integer.MAX_VALUE);
    testEncodeDecodeUnsignedLongValue(Long.MAX_VALUE);

    // negative values are inefficient in this encoding, but should still
    // work.
    testEncodeDecodeUnsignedLongValue(-123456L);
    testEncodeDecodeUnsignedLongValue(Byte.MIN_VALUE);
    testEncodeDecodeUnsignedLongValue(Integer.MIN_VALUE);
    testEncodeDecodeUnsignedLongValue(Long.MIN_VALUE);
  }

  private void testEncodeDecodeUnsignedLongValue(final long value) {
    final int length = VarintUtils.unsignedLongByteLength(value);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeUnsignedLong(value, buffer);
    buffer.position(0);
    final long decoded = VarintUtils.readUnsignedLong(buffer);
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testVarLongEncodeDecodeSignedLong() {
    testEncodeDecodeSignedLongValue(0L);
    testEncodeDecodeSignedLongValue(-123456L);
    testEncodeDecodeSignedLongValue(123456L);
    testEncodeDecodeSignedLongValue(Byte.MIN_VALUE);
    testEncodeDecodeSignedLongValue(Byte.MAX_VALUE);
    testEncodeDecodeSignedLongValue(Integer.MIN_VALUE);
    testEncodeDecodeSignedLongValue(Integer.MAX_VALUE);
    testEncodeDecodeSignedLongValue(Long.MIN_VALUE);
    testEncodeDecodeSignedLongValue(Long.MAX_VALUE);
  }

  private void testEncodeDecodeSignedLongValue(final long value) {
    final int length = VarintUtils.signedLongByteLength(value);
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeSignedLong(value, buffer);
    buffer.position(0);
    final long decoded = VarintUtils.readSignedLong(buffer);
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testEncodeDecodeTime() {
    final Calendar cal = Calendar.getInstance();
    // Current time
    testEncodeDecodeTimeValue(new Date());
    // Epoch time
    testEncodeDecodeTimeValue(new Date(0));
    // GeoWave epoch time
    testEncodeDecodeTimeValue(new Date(VarintUtils.TIME_EPOCH));
    // Distant past
    cal.set(15, Calendar.SEPTEMBER, 13, 5, 18, 36);
    testEncodeDecodeTimeValue(cal.getTime());
    // Distant future
    cal.set(3802, Calendar.DECEMBER, 31, 23, 59, 59);
    testEncodeDecodeTimeValue(cal.getTime());
  }

  private void testEncodeDecodeTimeValue(final Date value) {
    final int length = VarintUtils.timeByteLength(value.getTime());
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    VarintUtils.writeTime(value.getTime(), buffer);
    buffer.position(0);
    final Date decoded = new Date(VarintUtils.readTime(buffer));
    Assert.assertEquals(value, decoded);
  }

  @Test
  public void testEncodeDecodeBigDecimal() {
    testEncodeDecodeBigDecimalValue(new BigDecimal(123));
    testEncodeDecodeBigDecimalValue(new BigDecimal(-123));
    testEncodeDecodeBigDecimalValue(new BigDecimal(256));
    testEncodeDecodeBigDecimalValue(new BigDecimal(2_061_000_009));
    testEncodeDecodeBigDecimalValue(new BigDecimal(-1_000_000_000));
    testEncodeDecodeBigDecimalValue(new BigDecimal("3133731337313373133731337"));
    testEncodeDecodeBigDecimalValue(new BigDecimal("-3133731337313373133731337"));
    testEncodeDecodeBigDecimalValue(
        new BigDecimal("-3133731337313373133731337.3133731337313373133731337"));
  }

  private void testEncodeDecodeBigDecimalValue(final BigDecimal value) {
    byte[] encoded = VarintUtils.writeBigDecimal(value);
    BigDecimal roundtrip = VarintUtils.readBigDecimal(ByteBuffer.wrap(encoded));
    Assert.assertNotNull(roundtrip);
    Assert.assertEquals(0, value.compareTo(roundtrip));

    // append garbage after the BigDecimal to ensure that it is not read.
    byte[] garbage = new byte[] {0xc, 0xa, 0xf, 0xe, 32, 0xb, 0xa, 0xb, 0xe};
    ByteBuffer appended = ByteBuffer.allocate(encoded.length + garbage.length);
    appended.put(encoded);
    appended.put(garbage);
    roundtrip = VarintUtils.readBigDecimal((ByteBuffer) appended.flip());
    Assert.assertNotNull(roundtrip);
    Assert.assertEquals(
        value.toString() + " == " + roundtrip.toString(),
        0,
        value.compareTo(roundtrip));
  }
}
