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

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;

public class RowHistogramDataStaticticsTest
{
	static final long base = 7l;

	private GeoWaveKey genKey(
			final long id ) {
		final InsertionIds insertionIds = new InsertionIds(
				Arrays.asList(new ByteArrayId(
						String.format(
								"\12%5h",
								base + id) + "20030f89")));
		return GeoWaveKeyImpl.createKeys(
				insertionIds,
				new byte[] {},
				(short) 0)[0];
	}

	@Test
	public void testIngest() {
		final RowRangeHistogramStatistics<Integer> stats = new RowRangeHistogramStatistics<Integer>(
				(short) -1,
				new ByteArrayId(
						"20030"));

		for (long i = 0; i < 10000; i++) {
			final GeoWaveRow row = new GeoWaveRowImpl(
					genKey(i),
					new GeoWaveValue[] {});
			stats.entryIngested(
					1,
					row);
		}

		System.out.println(stats.toString());

		assertEquals(
				1.0,
				stats.cdf(
						null,
						genKey(
								10000).getSortKey()),
				0.00001);

		assertEquals(
				0.0,
				stats.cdf(
						null,
						genKey(
								0).getSortKey()),
				0.00001);

		assertEquals(
				0.5,
				stats.cdf(
						null,
						genKey(
								5000).getSortKey()),
				0.04);

		final RowRangeHistogramStatistics<Integer> stats2 = new RowRangeHistogramStatistics<Integer>(
				new ByteArrayId(
						"20030"));

		for (long j = 10000; j < 20000; j++) {

			final GeoWaveRow row = new GeoWaveRowImpl(
					genKey(j),
					new GeoWaveValue[] {});
			stats2.entryIngested(
					1,
					row);
		}

		assertEquals(
				0.0,
				stats2.cdf(
						null,
						genKey(
								10000).getSortKey()),
				0.00001);

		stats.merge(stats2);

		assertEquals(
				0.5,
				stats.cdf(
						null,
						genKey(
								10000).getSortKey()),
				0.15);

		stats2.fromBinary(stats.toBinary());

		assertEquals(
				0.5,
				stats.cdf(
						null,
						genKey(
								10000).getSortKey()),
				0.15);

		System.out.println(stats.toString());
	}
}
