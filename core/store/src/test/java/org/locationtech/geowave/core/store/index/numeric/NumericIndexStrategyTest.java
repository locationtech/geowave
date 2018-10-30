/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.index.numeric;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;

public class NumericIndexStrategyTest
{
	private final NumericFieldIndexStrategy strategy = new NumericFieldIndexStrategy();
	private final String fieldId = "fieldId";
	private final int number = 10;

	@Test
	public void testInsertions() {
		final InsertionIds insertionIds = strategy.getInsertionIds(number);
		final List<ByteArray> compositieInsertionIds = insertionIds.getCompositeInsertionIds();
		Assert.assertTrue(compositieInsertionIds.contains(new ByteArray(
				Lexicoders.DOUBLE.toByteArray((double) number))));
		Assert.assertTrue(compositieInsertionIds.size() == 1);
	}

	@Test
	public void testEquals() {
		final QueryRanges ranges = strategy.getQueryRanges(new NumericEqualsConstraint(
				fieldId,
				number));
		Assert.assertTrue(!ranges.isMultiRange());
		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArray(
								Lexicoders.DOUBLE.toByteArray((double) number)),
						new ByteArray(
								Lexicoders.DOUBLE.toByteArray((double) number)))));
	}

	@Test
	public void testGreaterThanOrEqualTo() {
		final QueryRanges ranges = strategy.getQueryRanges(new NumericGreaterThanOrEqualToConstraint(
				fieldId,
				number));
		Assert.assertTrue(!ranges.isMultiRange());
		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArray(
								Lexicoders.DOUBLE.toByteArray((double) number)),
						new ByteArray(
								Lexicoders.DOUBLE.toByteArray((double) Lexicoders.DOUBLE.getMaximumValue())))));
	}

	@Test
	public void testLessThanOrEqualTo() {
		final NumericFieldIndexStrategy strategy = new NumericFieldIndexStrategy();
		final QueryRanges ranges = strategy.getQueryRanges(new NumericLessThanOrEqualToConstraint(
				fieldId,
				number));
		Assert.assertTrue(!ranges.isMultiRange());

		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArray(
								Lexicoders.DOUBLE.toByteArray((double) Lexicoders.DOUBLE.getMinimumValue())),
						new ByteArray(
								Lexicoders.DOUBLE.toByteArray((double) number)))));
	}
}
