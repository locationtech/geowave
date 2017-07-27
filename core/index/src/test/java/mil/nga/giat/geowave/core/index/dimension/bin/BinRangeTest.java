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
package mil.nga.giat.geowave.core.index.dimension.bin;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;

import org.junit.Assert;
import org.junit.Test;

public class BinRangeTest
{

	private final double MINIMUM_RANGE = 20;
	private final double MAXIMUM_RANGE = 100;
	private double DELTA = 1e-15;

	@Test
	public void testBinRangeValues() {

		BinRange binRange = new BinRange(
				MINIMUM_RANGE,
				MAXIMUM_RANGE);

		Assert.assertEquals(
				MINIMUM_RANGE,
				binRange.getNormalizedMin(),
				DELTA);
		Assert.assertEquals(
				MAXIMUM_RANGE,
				binRange.getNormalizedMax(),
				DELTA);

		Assert.assertFalse(binRange.isFullExtent());

	}

	@Test
	public void testBinRangeFullExtent() {

		final int binIdValue = 120;
		final byte[] binID = ByteBuffer.allocate(
				4).putInt(
				binIdValue).array();
		final boolean fullExtent = true;

		BinRange binRange = new BinRange(
				binID,
				MINIMUM_RANGE,
				MAXIMUM_RANGE,
				fullExtent);

		Assert.assertEquals(
				MINIMUM_RANGE,
				binRange.getNormalizedMin(),
				DELTA);
		Assert.assertEquals(
				MAXIMUM_RANGE,
				binRange.getNormalizedMax(),
				DELTA);

		Assert.assertTrue(binRange.isFullExtent());

	}

}
