/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.data.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

public class BasicReaderWriterTest
{
	private Boolean booleanExpected;
	private Boolean booleanNullExpected;
	private Byte byteExpected;
	private Short shortExpected;
	private Short[] shortArrayExpected;
	private short[] primShortArrayExpected;
	private Float floatExpected;
	private Float[] floatArrayExpected;
	private float[] primFloatArrayExpected;
	private Double doubleExpected;
	private Double[] doubleArrayExpected;
	private double[] primDoubleArrayExpected;
	private BigDecimal bigDecimalExpected;
	private Integer integerExpected;
	private Integer[] intArrayExpected;
	private int[] primIntArrayExpected;
	private Long longExpected;
	private Long[] longArrayExpected;
	private long[] primLongArrayExpected;
	private BigInteger bigIntegerExpected;
	private String stringExpected;
	private String[] stringArrayExpected;
	private Byte[] byteArrayExpected;
	private byte[] primByteArrayExpected;
	private byte[] defaultNullExpected;

	public static void main(
			final String[] args ) {
		final BasicReaderWriterTest tester = new BasicReaderWriterTest();
		tester.init();
		tester.testBasicReadWrite();
	}

	@Before
	public void init() {
		booleanExpected = Boolean.TRUE;
		booleanNullExpected = Boolean.FALSE;
		byteExpected = Byte.MIN_VALUE;
		shortExpected = Short.MIN_VALUE;
		shortArrayExpected = new Short[] {
			Short.MIN_VALUE,
			null,
			Short.MAX_VALUE,
			null
		};
		primShortArrayExpected = new short[] {
			Short.MIN_VALUE,
			Short.MAX_VALUE
		};
		floatExpected = Float.MIN_VALUE;
		floatArrayExpected = new Float[] {
			null,
			Float.MIN_VALUE,
			null,
			Float.MAX_VALUE
		};
		primFloatArrayExpected = new float[] {
			Float.MIN_VALUE,
			Float.MAX_VALUE
		};
		doubleExpected = Double.MIN_VALUE;
		doubleArrayExpected = new Double[] {
			Double.MIN_VALUE,
			null,
			Double.MAX_VALUE,
			null
		};
		primDoubleArrayExpected = new double[] {
			Double.MIN_VALUE,
			Double.MAX_VALUE
		};
		bigDecimalExpected = BigDecimal.TEN;
		integerExpected = Integer.MIN_VALUE;
		intArrayExpected = new Integer[] {
			null,
			Integer.MIN_VALUE,
			null,
			Integer.MAX_VALUE
		};
		primIntArrayExpected = new int[] {
			Integer.MIN_VALUE,
			Integer.MAX_VALUE
		};
		longExpected = Long.MIN_VALUE;
		longArrayExpected = new Long[] {
			Long.MIN_VALUE,
			null,
			Long.MAX_VALUE,
			null
		};
		primLongArrayExpected = new long[] {
			Long.MIN_VALUE,
			Long.MAX_VALUE
		};
		bigIntegerExpected = BigInteger.valueOf(Long.MAX_VALUE);
		stringExpected = this.getClass().getName();
		stringArrayExpected = new String[] {
			null,
			this.getClass().getName(),
			null,
			String.class.getName()
		};
		byteArrayExpected = new Byte[] {
			Byte.MIN_VALUE,
			Byte.valueOf((byte) 55),
			Byte.MAX_VALUE
		};
		primByteArrayExpected = new byte[] {
			Byte.MIN_VALUE,
			(byte) 33,
			Byte.MAX_VALUE
		};
		defaultNullExpected = new byte[] {};
	}

	@Test
	public void testBasicReadWrite() {

		byte[] value;

		// test Boolean reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Boolean.class).writeField(
				booleanExpected);
		final Boolean booleanActual = FieldUtils.getDefaultReaderForClass(
				Boolean.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Boolean reader/writer",
				booleanExpected.booleanValue(),
				booleanActual.booleanValue());

		// test Byte reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Byte.class).writeField(
				byteExpected);
		final Byte byteActual = FieldUtils.getDefaultReaderForClass(
				Byte.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Byte reader/writer",
				byteExpected.byteValue(),
				byteActual.byteValue());

		// test Short reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Short.class).writeField(
				shortExpected);
		final Short shortActual = FieldUtils.getDefaultReaderForClass(
				Short.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Short reader/writer",
				shortExpected.shortValue(),
				shortActual.shortValue());

		// test Short Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Short[].class).writeField(
				shortArrayExpected);
		final Short[] shortArrayActual = FieldUtils.getDefaultReaderForClass(
				Short[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Short Array reader/writer",
				Arrays.deepEquals(
						shortArrayExpected,
						shortArrayActual));

		// test short Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				short[].class).writeField(
				primShortArrayExpected);
		final short[] primShortArrayActual = FieldUtils.getDefaultReaderForClass(
				short[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of short Array reader/writer",
				Arrays.equals(
						primShortArrayExpected,
						primShortArrayActual));

		// test Float reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Float.class).writeField(
				floatExpected);
		final Float floatActual = FieldUtils.getDefaultReaderForClass(
				Float.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Float reader/writer",
				floatExpected,
				floatActual);

		// test Float Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Float[].class).writeField(
				floatArrayExpected);
		final Float[] floatArrayActual = FieldUtils.getDefaultReaderForClass(
				Float[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Float Array reader/writer",
				Arrays.deepEquals(
						floatArrayExpected,
						floatArrayActual));

		// test float Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				float[].class).writeField(
				primFloatArrayExpected);
		final float[] primFloatArrayActual = FieldUtils.getDefaultReaderForClass(
				float[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of float Array reader/writer",
				Arrays.equals(
						primFloatArrayExpected,
						primFloatArrayActual));

		// test Double reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Double.class).writeField(
				doubleExpected);
		final Double doubleActual = FieldUtils.getDefaultReaderForClass(
				Double.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Double reader/writer",
				doubleExpected,
				doubleActual);

		// test Double Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Double[].class).writeField(
				doubleArrayExpected);
		final Double[] doubleArrayActual = FieldUtils.getDefaultReaderForClass(
				Double[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Double Array reader/writer",
				Arrays.deepEquals(
						doubleArrayExpected,
						doubleArrayActual));

		// test double Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				double[].class).writeField(
				primDoubleArrayExpected);
		final double[] primDoubleArrayActual = FieldUtils.getDefaultReaderForClass(
				double[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of double Array reader/writer",
				Arrays.equals(
						primDoubleArrayExpected,
						primDoubleArrayActual));

		// test BigDecimal reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				BigDecimal.class).writeField(
				bigDecimalExpected);
		final BigDecimal bigDecimalActual = FieldUtils.getDefaultReaderForClass(
				BigDecimal.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of BigDecimal reader/writer",
				bigDecimalExpected,
				bigDecimalActual);

		// test Integer reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Integer.class).writeField(
				integerExpected);
		final Integer integerActual = FieldUtils.getDefaultReaderForClass(
				Integer.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Integer reader/writer",
				integerExpected,
				integerActual);

		// test Integer Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Integer[].class).writeField(
				intArrayExpected);

		final Integer[] intArrayActual = FieldUtils.getDefaultReaderForClass(
				Integer[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Integer Array reader/writer",
				Arrays.deepEquals(
						intArrayExpected,
						intArrayActual));

		// test int Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				int[].class).writeField(
				primIntArrayExpected);
		final int[] primIntArrayActual = FieldUtils.getDefaultReaderForClass(
				int[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of int Array reader/writer",
				Arrays.equals(
						primIntArrayExpected,
						primIntArrayActual));

		// test Long reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Long.class).writeField(
				longExpected);
		final Long longActual = FieldUtils.getDefaultReaderForClass(
				Long.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Long reader/writer",
				longExpected,
				longActual);

		// test Long Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Long[].class).writeField(
				longArrayExpected);
		final Long[] longArrayActual = FieldUtils.getDefaultReaderForClass(
				Long[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Long Array reader/writer",
				Arrays.deepEquals(
						longArrayExpected,
						longArrayActual));

		// test long Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				long[].class).writeField(
				primLongArrayExpected);
		final long[] primLongArrayActual = FieldUtils.getDefaultReaderForClass(
				long[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of long Array reader/writer",
				Arrays.equals(
						primLongArrayExpected,
						primLongArrayActual));

		// test BigInteger reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				BigInteger.class).writeField(
				bigIntegerExpected);
		final BigInteger bigIntegerActual = FieldUtils.getDefaultReaderForClass(
				BigInteger.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of BigInteger reader/writer",
				bigIntegerExpected,
				bigIntegerActual);

		// test String reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				String.class).writeField(
				stringExpected);
		final String stringActual = FieldUtils.getDefaultReaderForClass(
				String.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of String reader/writer",
				stringExpected,
				stringActual);

		// test String Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				String[].class).writeField(
				stringArrayExpected);
		final String[] stringArrayActual = FieldUtils.getDefaultReaderForClass(
				String[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of String Array reader/writer",
				Arrays.deepEquals(
						stringArrayExpected,
						stringArrayActual));

		// test Byte [] reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Byte[].class).writeField(
				byteArrayExpected);
		final Byte[] byteArrayActual = FieldUtils.getDefaultReaderForClass(
				Byte[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Byte [] reader/writer",
				Arrays.deepEquals(
						byteArrayExpected,
						byteArrayActual));

		// test byte [] reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				byte[].class).writeField(
				primByteArrayExpected);
		final byte[] primByteArrayActual = FieldUtils.getDefaultReaderForClass(
				byte[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of byte [] reader/writer",
				Arrays.equals(
						primByteArrayExpected,
						primByteArrayActual));
	}

	@Test
	public void testNullReadWrite() {

		byte[] value;

		// test Boolean reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Boolean.class).writeField(
				null);
		final Boolean booleanNullActual = FieldUtils.getDefaultReaderForClass(
				Boolean.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of Boolean field writer/reader",
				booleanNullExpected.booleanValue(),
				booleanNullActual.booleanValue());

		// test Byte reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Byte.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Byte field writer",
				defaultNullExpected.length,
				value.length);
		final Byte byteActual = FieldUtils.getDefaultReaderForClass(
				Byte.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of Byte field reader",
				null,
				byteActual);

		// test Short reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Short.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Short writer",
				defaultNullExpected.length,
				value.length);
		final Short shortActual = FieldUtils.getDefaultReaderForClass(
				Short.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of Short reader",
				null,
				shortActual);

		// test Short Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Short[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Short Array writer",
				defaultNullExpected.length,
				value.length);
		final Short[] shortArrayActual = FieldUtils.getDefaultReaderForClass(
				Short[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Short Array reader",
				Arrays.deepEquals(
						null,
						shortArrayActual));

		// test short Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				short[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of short Array writer",
				defaultNullExpected.length,
				value.length);
		final short[] primShortArrayActual = FieldUtils.getDefaultReaderForClass(
				short[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of short Array reader",
				Arrays.equals(
						null,
						primShortArrayActual));

		// test Float reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Float.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Float writer",
				defaultNullExpected.length,
				value.length);
		final Float floatActual = FieldUtils.getDefaultReaderForClass(
				Float.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of Float Array reader",
				null,
				floatActual);

		// test Float Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Float[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Float Array writer",
				defaultNullExpected.length,
				value.length);
		final Float[] floatArrayActual = FieldUtils.getDefaultReaderForClass(
				Float[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of Float Array reader",
				Arrays.deepEquals(
						null,
						floatArrayActual));

		// // test float Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				float[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of float Array writer",
				defaultNullExpected.length,
				value.length);
		final float[] primFloatArrayActual = FieldUtils.getDefaultReaderForClass(
				float[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of float Array reader/writer",
				Arrays.equals(
						null,
						primFloatArrayActual));

		// test Double reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Double.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Double writer",
				defaultNullExpected.length,
				value.length);
		final Double doubleActual = FieldUtils.getDefaultReaderForClass(
				Double.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of Double reader",
				null,
				doubleActual);

		// test Double Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Double[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Double Array writer",
				defaultNullExpected.length,
				value.length);
		final Double[] doubleArrayActual = FieldUtils.getDefaultReaderForClass(
				Double[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of Double Array reader",
				Arrays.deepEquals(
						null,
						doubleArrayActual));

		// test double Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				double[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of double Array writer",
				defaultNullExpected.length,
				value.length);
		final double[] primDoubleArrayActual = FieldUtils.getDefaultReaderForClass(
				double[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of double Array reader",
				Arrays.equals(
						null,
						primDoubleArrayActual));

		// test BigDecimal reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				BigDecimal.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of BigDecimal writer",
				defaultNullExpected.length,
				value.length);
		final BigDecimal bigDecimalActual = FieldUtils.getDefaultReaderForClass(
				BigDecimal.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of BigDecimal reader",
				null,
				bigDecimalActual);

		// test Integer reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Integer.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Integer writer",
				defaultNullExpected.length,
				value.length);
		final Integer integerActual = FieldUtils.getDefaultReaderForClass(
				Integer.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Integer reader",
				null,
				integerActual);

		// test Integer Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Integer[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Integer Array writer",
				defaultNullExpected.length,
				value.length);
		final Integer[] intArrayActual = FieldUtils.getDefaultReaderForClass(
				Integer[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of Integer Array reader",
				Arrays.deepEquals(
						null,
						intArrayActual));

		// test int Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				int[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of int Array writer",
				defaultNullExpected.length,
				value.length);
		final int[] primIntArrayActual = FieldUtils.getDefaultReaderForClass(
				int[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of int Array reader",
				Arrays.equals(
						null,
						primIntArrayActual));

		// test Long reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Long.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Long writer",
				defaultNullExpected.length,
				value.length);
		final Long longActual = FieldUtils.getDefaultReaderForClass(
				Long.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Long reader",
				null,
				longActual);

		// test Long Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Long[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Long Array writer",
				defaultNullExpected.length,
				value.length);
		final Long[] longArrayActual = FieldUtils.getDefaultReaderForClass(
				Long[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Long Array reader",
				Arrays.deepEquals(
						null,
						longArrayActual));

		// test long Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				long[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of long Array writer",
				defaultNullExpected.length,
				value.length);
		final long[] primLongArrayActual = FieldUtils.getDefaultReaderForClass(
				long[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of long Array reader/writer",
				Arrays.equals(
						null,
						primLongArrayActual));

		// test BigInteger reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				BigInteger.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of BigInteger writer",
				defaultNullExpected.length,
				value.length);
		final BigInteger bigIntegerActual = FieldUtils.getDefaultReaderForClass(
				BigInteger.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of BigInteger reader",
				null,
				bigIntegerActual);

		// test String reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				String.class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of String writer",
				defaultNullExpected.length,
				value.length);
		final String stringActual = FieldUtils.getDefaultReaderForClass(
				String.class).readField(
				value);
		Assert.assertEquals(
				"FAILED null test of String reader/writer",
				null,
				stringActual);

		// test String Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				String[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of String Array writer",
				defaultNullExpected.length,
				value.length);
		final String[] stringArrayActual = FieldUtils.getDefaultReaderForClass(
				String[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of String Array reader/writer",
				Arrays.deepEquals(
						null,
						stringArrayActual));

		// test Byte [] reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Byte[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of Byte [] writer",
				defaultNullExpected.length,
				value.length);
		final Byte[] byteArrayActual = FieldUtils.getDefaultReaderForClass(
				Byte[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of Byte [] reader",
				Arrays.deepEquals(
						null,
						byteArrayActual));

		// test byte [] reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				byte[].class).writeField(
				null);
		Assert.assertEquals(
				"FAILED null test of byte [] writer",
				defaultNullExpected.length,
				value.length);
		final byte[] primByteArrayActual = FieldUtils.getDefaultReaderForClass(
				byte[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED null test of byte [] reader/writer",
				Arrays.equals(
						null,
						primByteArrayActual));
	}

}
