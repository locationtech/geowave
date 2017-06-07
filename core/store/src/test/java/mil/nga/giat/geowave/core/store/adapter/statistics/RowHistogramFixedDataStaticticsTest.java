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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

public class RowHistogramFixedDataStaticticsTest
{
	static final long base = 7l;

	private ByteArrayId genId(
			final long id ) {
		return new ByteArrayId(
				String.format(
						"\12%5h",
						base + id) + "20030f89");
	}

	@Test
	public void testIngest() {
		final RowRangeHistogramStatistics<Integer> stats = new RowRangeHistogramStatistics<Integer>(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"20030"),
				1024);

		for (long i = 0; i < 10000; i++) {
			final List<ByteArrayId> ids = Arrays.asList(genId(i));
			stats.entryIngested(
					new DataStoreEntryInfo(
							Long.toString(
									i).getBytes(),
							ids,
							ids,
							Collections.<FieldInfo<?>> emptyList()),
					1);
		}

		System.out.println(stats.toString());

		assertEquals(
				1.0,
				stats.cdf(genId(
						10000).getBytes()),
				0.00001);

		assertEquals(
				0.0,
				stats.cdf(genId(
						0).getBytes()),
				0.00001);

		assertEquals(
				0.5,
				stats.cdf(genId(
						5000).getBytes()),
				0.04);

		final RowRangeHistogramStatistics<Integer> stats2 = new RowRangeHistogramStatistics<Integer>(
				new ByteArrayId(
						"20030"),
				new ByteArrayId(
						"20030"),
				1024);

		for (long j = 10000; j < 20000; j++) {

			ByteArrayId id = genId(j);
			stats2.entryIngested(
					new DataStoreEntryInfo(
							Long.toString(
									j).getBytes(),
							Arrays.asList(id),
							Arrays.asList(id),
							Collections.<FieldInfo<?>> emptyList()),
					1);
		}

		assertEquals(
				0.0,
				stats2.cdf(genId(
						10000).getBytes()),
				0.00001);

		stats.merge(stats2);

		assertEquals(
				0.5,
				stats.cdf(genId(
						10000).getBytes()),
				0.15);

		stats2.fromBinary(stats.toBinary());

		assertEquals(
				0.5,
				stats.cdf(genId(
						10000).getBytes()),
				0.15);

		System.out.println(stats.toString());
	}
}
