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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Test;

public class TemporalIndexStrategyTest
{
	private final TemporalIndexStrategy strategy = new TemporalIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final Date date = new Date(
			1440080038544L);

	@Test
	public void testInsertions() {
		final List<FieldInfo<Date>> fieldInfoList = new ArrayList<>();
		final FieldInfo<Date> fieldInfo = new FieldInfo<>(
				new PersistentValue<Date>(
						null,
						date),
				null,
				null);
		fieldInfoList.add(fieldInfo);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(fieldInfoList);
		Assert.assertTrue(insertionIds.size() == 1);
		Assert.assertTrue(insertionIds.contains(new ByteArrayId(
				Lexicoders.LONG.toByteArray(date.getTime()))));
	}

	@Test
	public void testDateRange() {
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(new TemporalQueryConstraint(
				fieldId,
				date,
				date));
		Assert.assertTrue(ranges.size() == 1);
		Assert.assertTrue(ranges.get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								Lexicoders.LONG.toByteArray(date.getTime())),
						new ByteArrayId(
								Lexicoders.LONG.toByteArray(date.getTime())))));
	}

}
