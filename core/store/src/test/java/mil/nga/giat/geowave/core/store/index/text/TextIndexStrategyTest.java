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
package mil.nga.giat.geowave.core.store.index.text;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;

public class TextIndexStrategyTest
{
	private final TextIndexStrategy strategy = new TextIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final String value = "myString";

	@Test
	public void testInsertions() {
		final InsertionIds insertionIds = strategy.getInsertionIds(value);
		Assert.assertTrue(insertionIds.getCompositeInsertionIds().contains(
				new ByteArrayId(
						value)));
		Assert.assertTrue(insertionIds.getCompositeInsertionIds().size() == 1);
	}

	@Test
	public void testEquals() {
		final QueryRanges ranges = strategy.getQueryRanges(new TextQueryConstraint(
				fieldId,
				value,
				false));
		Assert.assertTrue(ranges.getCompositeQueryRanges().size() == 1);
		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								value),
						new ByteArrayId(
								value))));
	}
}
