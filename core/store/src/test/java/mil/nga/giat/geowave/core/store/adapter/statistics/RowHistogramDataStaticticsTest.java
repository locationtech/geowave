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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.FixedBinNumericHistogram.FixedBinNumericHistogramFactory;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

import org.junit.Test;

public class RowHistogramDataStaticticsTest
{

	Random r = new Random(
			347);

	private ByteArrayId genId(
			long bottom,
			long top ) {
		return new ByteArrayId(
				String.format(
						"\12%6h",
						bottom + (r.nextDouble() * (top - bottom))) + "20030f89");
	}

	RowRangeHistogramStatistics<Integer> stats1 = new RowRangeHistogramStatistics<Integer>(
			new ByteArrayId(
					"20030"),
			new ByteArrayId(
					"20030"),
			new FixedBinNumericHistogramFactory(),
			1024);

	RowRangeHistogramStatistics<Integer> stats2 = new RowRangeHistogramStatistics<Integer>(
			new ByteArrayId(
					"20030"),
			new ByteArrayId(
					"20030"));

	@Test
	public void testId() {
		assertEquals(
				stats1.getStatisticsId(),
				stats1.duplicate().getStatisticsId());

	}

	@Test
	public void testIngest() {

		for (long i = 0; i < 10000; i++) {
			List<ByteArrayId> ids = Arrays.asList(genId(
					0,
					100000));
			stats1.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							ids,
							ids,
							Collections.<FieldInfo<?>> emptyList()),
					1);
			stats2.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							ids,
							ids,
							Collections.<FieldInfo<?>> emptyList()),
					1);
		}

		for (int i = 1000; i < 100000; i += 1000) {
			byte[] half = genId(
					i,
					i + 1).getBytes();
			double diff = Math.abs(stats1.cdf(half) - stats2.cdf(half));
			assertTrue(
					"iteration " + i + " = " + diff,
					diff < 0.02);
		}

		System.out.println("-------------------------");

		for (long j = 10000; j < 20000; j++) {
			List<ByteArrayId> ids = Arrays.asList(genId(
					100000,
					200000));
			stats1.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							ids,
							ids,
							Collections.<FieldInfo<?>> emptyList()),
					1);
			stats2.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							ids,
							ids,
							Collections.<FieldInfo<?>> emptyList()),
					1);
		}

		for (int i = 1000; i < 100000; i += 1000) {
			byte[] half = genId(
					i,
					i + 1).getBytes();
			double diff = Math.abs(stats1.cdf(half) - stats2.cdf(half));
			assertTrue(
					"iteration " + i + " = " + diff,
					diff < 0.02);
		}

		byte[] nearfull = genId(
				79998,
				89999).getBytes();
		double diff = Math.abs(stats1.cdf(nearfull) - stats2.cdf(nearfull));
		assertTrue(
				"nearfull = " + diff,
				diff < 0.02);
		byte[] nearempty = genId(
				9998,
				9999).getBytes();
		diff = Math.abs(stats1.cdf(nearempty) - stats2.cdf(nearempty));
		assertTrue(
				"nearempty = " + diff,
				diff < 0.02);
	}
}
