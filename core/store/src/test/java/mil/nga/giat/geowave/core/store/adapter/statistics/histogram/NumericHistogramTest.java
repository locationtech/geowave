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
package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram;

import org.junit.Test;

public class NumericHistogramTest
{

	Random r = new Random(
			347);

	MinimalBinDistanceHistogram stats = new MinimalBinDistanceHistogram();
	FixedBinNumericHistogram stats2 = new FixedBinNumericHistogram();

	@Test
	public void testIngest() {

		for (long i = 0; i < 10000; i++) {
			double v = 2500 + (r.nextDouble() * 99998.0);
			stats.add(v);
			stats2.add(v);
		}

		assertEquals(
				0,
				stats.cdf(2500),
				0.001);
		assertEquals(
				1.0,
				stats.cdf(102500),
				0.001);
		assertEquals(
				0.5,
				stats.cdf(52500),
				0.02);

		assertEquals(
				0,
				stats2.cdf(2500),
				0.001);
		assertEquals(
				1.0,
				stats2.cdf(102500),
				0.001);
		assertEquals(
				0.5,
				stats2.cdf(52500),
				0.02);

		assertEquals(
				27,
				stats.quantile(0.25) / 1000.0,
				0.1);
		assertEquals(
				52,
				stats.quantile(0.5) / 1000.0,
				0.3);
		assertEquals(
				78,
				stats.quantile(0.75) / 1000.0,
				0.3);

		assertEquals(
				55,
				stats2.quantile(0.5) / 1000.0,
				1.0);
		assertEquals(
				81,
				stats2.quantile(0.75) / 1000.0,
				0.1);
	}
}
