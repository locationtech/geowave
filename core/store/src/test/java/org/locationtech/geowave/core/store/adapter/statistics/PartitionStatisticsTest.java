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
package org.locationtech.geowave.core.store.adapter.statistics;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class PartitionStatisticsTest
{
	static final long base = 7l;
	static int counter = 0;

	private GeoWaveKey genKey(
			final long id ) {
		final InsertionIds insertionIds = new InsertionIds(
				new ByteArray(
						new byte[] {
							(byte) (counter++ % 32)
						}),
				Arrays.asList(new ByteArray(
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
		final PartitionStatistics<Integer> stats = new PartitionStatistics<>(
				(short) 20030,
				"20030");

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
				32,
				stats.getPartitionKeys().size());
		for (byte i = 0; i < 32; i++) {
			Assert.assertTrue(stats.getPartitionKeys().contains(
					new ByteArray(
							new byte[] {
								i
							})));
		}
	}
}
