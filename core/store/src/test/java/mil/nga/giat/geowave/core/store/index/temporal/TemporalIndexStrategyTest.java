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
package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.Date;

import org.junit.Test;

import org.junit.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;

public class TemporalIndexStrategyTest
{
	private final TemporalIndexStrategy strategy = new TemporalIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final Date date = new Date(
			1440080038544L);

	@Test
	public void testInsertions() {
		final InsertionIds insertionIds = strategy.getInsertionIds(date);
		Assert.assertTrue(insertionIds.getSize() == 1);
		Assert.assertTrue(insertionIds.getCompositeInsertionIds().contains(
				new ByteArrayId(
						Lexicoders.LONG.toByteArray(date.getTime()))));
	}

	@Test
	public void testDateRange() {
		final QueryRanges ranges = strategy.getQueryRanges(new TemporalQueryConstraint(
				fieldId,
				date,
				date));
		Assert.assertTrue(ranges.getCompositeQueryRanges().size() == 1);
		Assert.assertTrue(ranges.getCompositeQueryRanges().get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.LONG.toByteArray(date.getTime())),
						new ByteArrayId(
								Lexicoders.LONG.toByteArray(date.getTime())))));
	}

}
