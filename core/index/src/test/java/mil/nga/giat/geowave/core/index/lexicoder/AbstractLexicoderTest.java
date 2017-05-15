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
package mil.nga.giat.geowave.core.index.lexicoder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractLexicoderTest<T extends Number & Comparable<T>>
{
	private NumberLexicoder<T> lexicoder;
	private T expectedMin;
	private T expectedMax;
	private T[] unsortedVals;
	private Comparator<byte[]> comparator;

	public AbstractLexicoderTest(
			final NumberLexicoder<T> lexicoder,
			final T expectedMin,
			final T expectedMax,
			final T[] unsortedVals,
			final Comparator<byte[]> comparator ) {
		super();
		this.lexicoder = lexicoder;
		this.expectedMin = expectedMin;
		this.expectedMax = expectedMax;
		this.unsortedVals = unsortedVals;
		this.comparator = comparator;
	}

	@Test
	public void testRanges() {
		Assert.assertTrue(lexicoder.getMinimumValue().equals(
				expectedMin));
		Assert.assertTrue(lexicoder.getMaximumValue().equals(
				expectedMax));
	}

	@Test
	public void testSortOrder() {
		final List<T> list = Arrays.asList(unsortedVals);
		final Map<byte[], T> sortedByteArrayToRawTypeMappings = new TreeMap<>(
				comparator);
		for (final T d : list) {
			sortedByteArrayToRawTypeMappings.put(
					lexicoder.toByteArray(d),
					d);
		}
		Collections.sort(list);
		int idx = 0;
		final Set<byte[]> sortedByteArrays = sortedByteArrayToRawTypeMappings.keySet();
		for (final byte[] byteArray : sortedByteArrays) {
			final T value = sortedByteArrayToRawTypeMappings.get(byteArray);
			Assert.assertTrue(value.equals(list.get(idx++)));
		}
	}

}
