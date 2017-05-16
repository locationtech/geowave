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
package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Assert;
import org.junit.Test;

public class NumericIndexStrategyTest
{
	private final NumericFieldIndexStrategy strategy = new NumericFieldIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final int number = 10;

	@Test
	public void testInsertions() {
		final List<FieldInfo<Number>> fieldInfoList = new ArrayList<>();
		final FieldInfo<Number> fieldInfo = new FieldInfo<>(
				new PersistentValue<Number>(
						null,
						number),
				null,
				null);
		fieldInfoList.add(fieldInfo);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(fieldInfoList);
		Assert.assertTrue(insertionIds.contains(new ByteArrayId(
				Lexicoders.DOUBLE.toByteArray((double) number))));
		Assert.assertTrue(insertionIds.size() == 1);
	}

	@Test
	public void testEquals() {
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(new NumericEqualsConstraint(
				fieldId,
				number));
		Assert.assertTrue(ranges.size() == 1);
		Assert.assertTrue(ranges.get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)),
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)))));
	}

	@Test
	public void testGreaterThanOrEqualTo() {
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(new NumericGreaterThanOrEqualToConstraint(
				fieldId,
				number));
		Assert.assertTrue(ranges.size() == 1);
		Assert.assertTrue(ranges.get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)),
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) Lexicoders.DOUBLE.getMaximumValue())))));
	}

	@Test
	public void testLessThanOrEqualTo() {
		final NumericFieldIndexStrategy strategy = new NumericFieldIndexStrategy();
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(new NumericLessThanOrEqualToConstraint(
				fieldId,
				number));
		Assert.assertTrue(ranges.size() == 1);

		Assert.assertTrue(ranges.get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) Lexicoders.DOUBLE.getMinimumValue())),
						new ByteArrayId(
								Lexicoders.DOUBLE.toByteArray((double) number)))));
	}
}
