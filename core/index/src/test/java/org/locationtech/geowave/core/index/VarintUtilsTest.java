/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership. All rights reserved. This program and the accompanying materials are made available under the terms of the Apache License, Version 2.0 which accompanies this distribution and is available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class VarintUtilsTest
{

	@Test
	public void testVarintEncodeDecodeUnsignedIntReversed() {
		testEncodeDecodeUnsignedIntReversed(0);
		testEncodeDecodeUnsignedIntReversed(123456);
		testEncodeDecodeUnsignedIntReversed(Byte.MAX_VALUE);
		testEncodeDecodeUnsignedIntReversed(Integer.MAX_VALUE);

		int length = VarintUtils.unsignedIntByteLength(15) + VarintUtils.unsignedIntByteLength(Byte.MAX_VALUE);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeUnsignedIntReversed(
				15,
				buffer);
		VarintUtils.writeUnsignedIntReversed(
				Byte.MAX_VALUE,
				buffer);
		buffer.position(buffer.limit() - 1);
		Assert.assertEquals(
				Byte.MAX_VALUE,
				VarintUtils.readUnsignedIntReversed(buffer));
		Assert.assertEquals(
				15,
				VarintUtils.readUnsignedIntReversed(buffer));

	}

	private void testEncodeDecodeUnsignedIntReversed(
			int value ) {
		int length = VarintUtils.unsignedIntByteLength(value);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeUnsignedIntReversed(
				value,
				buffer);
		buffer.position(buffer.limit() - 1);
		int decoded = VarintUtils.readUnsignedIntReversed(buffer);
		Assert.assertEquals(
				value,
				decoded);
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

	private void testSignedUnsignedIntValue(
			int value ) {
		int unsigned = VarintUtils.signedToUnsignedInt(value);
		int signed = VarintUtils.unsignedToSignedInt(unsigned);
		Assert.assertEquals(
				value,
				signed);
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

	private void testEncodeDecodeUnsignedIntValue(
			int value ) {
		int length = VarintUtils.unsignedIntByteLength(value);
		// should never use more than 5 bytes
		Assert.assertTrue(length <= 5);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeUnsignedInt(
				value,
				buffer);
		buffer.position(0);
		int decoded = VarintUtils.readUnsignedInt(buffer);
		Assert.assertEquals(
				value,
				decoded);
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

	private void testEncodeDecodeUnsignedShortValue(
			short value ) {
		int length = VarintUtils.unsignedShortByteLength(value);
		// should never use more than 3 bytes
		Assert.assertTrue(length <= 3);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeUnsignedShort(
				value,
				buffer);
		buffer.position(0);
		int decoded = VarintUtils.readUnsignedShort(buffer);
		Assert.assertEquals(
				value,
				decoded);
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

	private void testEncodeDecodeSignedIntValue(
			int value ) {
		int length = VarintUtils.signedIntByteLength(value);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeSignedInt(
				value,
				buffer);
		buffer.position(0);
		int decoded = VarintUtils.readSignedInt(buffer);
		Assert.assertEquals(
				value,
				decoded);
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

	private void testSignedUnsignedLongValue(
			long value ) {
		long unsigned = VarintUtils.signedToUnsignedLong(value);
		long signed = VarintUtils.unsignedToSignedLong(unsigned);
		Assert.assertEquals(
				value,
				signed);
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

	private void testEncodeDecodeUnsignedLongValue(
			long value ) {
		int length = VarintUtils.unsignedLongByteLength(value);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeUnsignedLong(
				value,
				buffer);
		buffer.position(0);
		long decoded = VarintUtils.readUnsignedLong(buffer);
		Assert.assertEquals(
				value,
				decoded);
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

	private void testEncodeDecodeSignedLongValue(
			long value ) {
		int length = VarintUtils.signedLongByteLength(value);
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeSignedLong(
				value,
				buffer);
		buffer.position(0);
		long decoded = VarintUtils.readSignedLong(buffer);
		Assert.assertEquals(
				value,
				decoded);
	}

	@Test
	public void testEncodeDecodeTime() {
		final Calendar cal = Calendar.getInstance();
		// Current time
		testEncodeDecodeTimeValue(new Date());
		// Epoch time
		testEncodeDecodeTimeValue(new Date(
				0));
		// Geowave epoch time
		testEncodeDecodeTimeValue(new Date(
				VarintUtils.TIME_EPOCH));
		// Distant past
		cal.set(
				15,
				8,
				13,
				5,
				18,
				36);
		testEncodeDecodeTimeValue(cal.getTime());
		// Distant future
		cal.set(
				3802,
				11,
				31,
				23,
				59,
				59);
		testEncodeDecodeTimeValue(cal.getTime());
	}

	private void testEncodeDecodeTimeValue(
			Date value ) {
		int length = VarintUtils.timeByteLength(value.getTime());
		ByteBuffer buffer = ByteBuffer.allocate(length);
		VarintUtils.writeTime(
				value.getTime(),
				buffer);
		buffer.position(0);
		Date decoded = new Date(
				VarintUtils.readTime(buffer));
		Assert.assertEquals(
				value,
				decoded);
	}
}
