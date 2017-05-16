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
package mil.nga.giat.geowave.core.index.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.primitives.UnsignedBytes;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;

@RunWith(Parameterized.class)
public class SimpleNumericIndexStrategyTest
{

	private final SimpleNumericIndexStrategy<? extends Number> strategy;

	public SimpleNumericIndexStrategyTest(
			final SimpleNumericIndexStrategy<?> strategy ) {
		this.strategy = strategy;
	}

	@Parameters
	public static Collection<Object[]> instancesToTest() {
		return Arrays.asList(
				new Object[] {
					new SimpleShortIndexStrategy()
				},
				new Object[] {
					new SimpleIntegerIndexStrategy()
				},
				new Object[] {
					new SimpleLongIndexStrategy()
				});
	}

	private static long castToLong(
			final Number n ) {
		if (n instanceof Short) {
			return ((short) n.shortValue());
		}
		else if (n instanceof Integer) {
			return ((int) n.intValue());
		}
		else if (n instanceof Long) {
			return (long) n.longValue();
		}
		else {
			throw new UnsupportedOperationException(
					"only supports casting Short, Integer, and Long");
		}
	}

	private static MultiDimensionalNumericData getIndexedRange(
			final long value ) {
		return getIndexedRange(
				value,
				value);
	}

	private static MultiDimensionalNumericData getIndexedRange(
			final long min,
			final long max ) {
		NumericData[] dataPerDimension;
		if (min == max) {
			dataPerDimension = new NumericData[] {
				new NumericValue(
						min)
			};
		}
		else {
			dataPerDimension = new NumericData[] {
				new NumericRange(
						min,
						max)
			};
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}

	private byte[] getByteArray(
			final long value ) {
		final MultiDimensionalNumericData indexedRange = getIndexedRange(value);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(indexedRange);
		final ByteArrayId insertionId = insertionIds.get(0);
		return insertionId.getBytes();
	}

	@Test
	public void testGetQueryRangesPoint() {
		final MultiDimensionalNumericData indexedRange = getIndexedRange(10l);
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(indexedRange);
		Assert.assertEquals(
				ranges.size(),
				1);
		final ByteArrayRange range = ranges.get(0);
		final ByteArrayId start = range.getStart();
		final ByteArrayId end = range.getEnd();
		Assert.assertTrue(Arrays.equals(
				start.getBytes(),
				end.getBytes()));
		Assert.assertEquals(
				10L,
				castToLong(strategy.getLexicoder().fromByteArray(
						start.getBytes())));
	}

	@Test
	public void testGetQueryRangesRange() {
		final long startValue = 10;
		final long endValue = 15;
		final MultiDimensionalNumericData indexedRange = getIndexedRange(
				startValue,
				endValue);
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(indexedRange);
		Assert.assertEquals(
				ranges.size(),
				1);
		final ByteArrayRange range = ranges.get(0);
		final ByteArrayId start = range.getStart();
		final ByteArrayId end = range.getEnd();
		Assert.assertEquals(
				castToLong(strategy.getLexicoder().fromByteArray(
						start.getBytes())),
				startValue);
		Assert.assertEquals(
				castToLong(strategy.getLexicoder().fromByteArray(
						end.getBytes())),
				endValue);
	}

	/**
	 * Check that lexicographical sorting of the byte arrays yields the same
	 * sort order as sorting the values
	 */
	@Test
	public void testRangeSortOrder() {
		final List<Long> values = Arrays.asList(
				10l,
				0l,
				15l,
				-275l,
				982l,
				430l,
				-1l,
				1l,
				82l);
		final List<byte[]> byteArrays = new ArrayList<>(
				values.size());
		for (final long value : values) {
			final byte[] bytes = getByteArray(value);
			byteArrays.add(bytes);
		}
		Collections.sort(values);
		Collections.sort(
				byteArrays,
				UnsignedBytes.lexicographicalComparator());
		final List<Long> convertedValues = new ArrayList<>(
				values.size());
		for (final byte[] bytes : byteArrays) {
			final long value = castToLong(strategy.getLexicoder().fromByteArray(
					bytes));
			convertedValues.add(value);
		}
		Assert.assertTrue(values.equals(convertedValues));
	}

	@Test
	public void testGetInsertionIdsPoint() {
		final long pointValue = 5926;
		final MultiDimensionalNumericData indexedData = getIndexedRange(pointValue);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(indexedData);
		Assert.assertEquals(
				insertionIds.size(),
				1);
		final ByteArrayId insertionId = insertionIds.get(0);
		Assert.assertEquals(
				castToLong(strategy.getLexicoder().fromByteArray(
						insertionId.getBytes())),
				pointValue);
	}

	@Test
	public void testGetInsertionIdsRange() {
		final long startValue = 9876;
		final long endValue = startValue + 15;
		final MultiDimensionalNumericData indexedData = getIndexedRange(
				startValue,
				endValue);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(indexedData);
		Assert.assertEquals(
				insertionIds.size(),
				(int) ((endValue - startValue) + 1));
		int i = 0;
		for (final ByteArrayId insertionId : insertionIds) {
			Assert.assertEquals(
					castToLong(strategy.getLexicoder().fromByteArray(
							insertionId.getBytes())),
					startValue + i++);
		}
	}
}
