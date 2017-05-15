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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

public class RowRangeDataStaticticsTest
{

	@Test
	public void testEmpty() {
		final RowRangeDataStatistics<Integer> stats = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		assertFalse(stats.isSet());

		stats.fromBinary(stats.toBinary());
	}

	@Test
	public void testIngest() {
		final RowRangeDataStatistics<Integer> stats = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		List<ByteArrayId> ids = Arrays.asList(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"014"),
				new ByteArrayId(
						"0124"),
				new ByteArrayId(
						"0123"),
				new ByteArrayId(
						"5064"),
				new ByteArrayId(
						"50632"));
		stats.entryIngested(
				new DataStoreEntryInfo(
						"23".getBytes(),
						ids,
						ids,
						Collections.<FieldInfo<?>> emptyList()),
				1);

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"0123").getBytes(),
				stats.getMin()));

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"5064").getBytes(),
				stats.getMax()));

		assertTrue(stats.isSet());

		// merge

		final RowRangeDataStatistics<Integer> stats2 = new RowRangeDataStatistics<Integer>(
				new ByteArrayId(
						"20030"));
		ids = Arrays.asList(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"014"),
				new ByteArrayId(
						"8062"));
		stats2.entryIngested(
				new DataStoreEntryInfo(
						"32".getBytes(),
						ids,
						ids,
						Collections.<FieldInfo<?>> emptyList()),
				1);

		stats.merge(stats2);

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"0123").getBytes(),
				stats.getMin()));

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"8062").getBytes(),
				stats.getMax()));

		stats2.fromBinary(stats.toBinary());

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"0123").getBytes(),
				stats2.getMin()));

		assertTrue(Arrays.equals(
				new ByteArrayId(
						"8062").getBytes(),
				stats2.getMax()));

		stats.toString();
	}
}
