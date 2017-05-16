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

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import org.junit.Assert;
import org.junit.Test;

public class TextIndexStrategyTest
{
	private final TextIndexStrategy strategy = new TextIndexStrategy();
	private final ByteArrayId fieldId = new ByteArrayId(
			"fieldId");
	private final String value = "myString";

	@Test
	public void testInsertions() {
		final List<FieldInfo<String>> fieldInfoList = new ArrayList<>();
		final FieldInfo<String> fieldInfo = new FieldInfo<>(
				new PersistentValue<String>(
						null,
						value),
				null,
				null);
		fieldInfoList.add(fieldInfo);
		final List<ByteArrayId> insertionIds = strategy.getInsertionIds(fieldInfoList);
		Assert.assertTrue(insertionIds.contains(new ByteArrayId(
				value)));
		Assert.assertTrue(insertionIds.size() == 1);
	}

	@Test
	public void testEquals() {
		final List<ByteArrayRange> ranges = strategy.getQueryRanges(new TextQueryConstraint(
				fieldId,
				value,
				false));
		Assert.assertTrue(ranges.size() == 1);
		Assert.assertTrue(ranges.get(
				0).equals(
				new ByteArrayRange(
						new ByteArrayId(
								value),
						new ByteArrayId(
								value))));
	}
}
