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
package mil.nga.giat.geowave.core.geotime.index.dimension;

import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.junit.Assert;
import org.junit.Test;

public class LongitudeDefinitionTest
{

	private double DELTA = 1e-15;

	@Test
	public void testNormalizeWithinBoundsRanges() {

		final double minRange = 10;
		final double maxRange = 100;

		BinRange[] binRange = getNormalizedRanges(
				minRange,
				maxRange);

		Assert.assertEquals(
				minRange,
				binRange[0].getNormalizedMin(),
				DELTA);

		Assert.assertEquals(
				maxRange,
				binRange[0].getNormalizedMax(),
				DELTA);

	}

	@Test
	public void testNormalizeWithinBoundsValue() {

		final double easternNormalizedValue = -160;
		final double westernNormalizedValue = 160;

		final double easternValue = 200;
		final double westernValue = -200;

		Assert.assertEquals(
				easternNormalizedValue,
				getNormalizedLongitudeValue(easternValue),
				DELTA);

		Assert.assertEquals(
				westernNormalizedValue,
				getNormalizedLongitudeValue(westernValue),
				DELTA);

	}

	@Test
	public void testNormalizeDateLineCrossingEast() {

		final double minRange = 150;
		final double maxRange = 200;

		final int expectedBinCount = 2;

		final BinRange[] expectedBinRanges = new BinRange[] {
			new BinRange(
					-180,
					-160),
			new BinRange(
					150,
					180)
		};

		BinRange[] binRange = getNormalizedRanges(
				minRange,
				maxRange);

		Assert.assertEquals(
				expectedBinCount,
				binRange.length);

		for (int i = 0; i < binRange.length; i++) {

			Assert.assertEquals(
					expectedBinRanges[i].getNormalizedMin(),
					binRange[i].getNormalizedMin(),
					DELTA);

			Assert.assertEquals(
					expectedBinRanges[i].getNormalizedMax(),
					binRange[i].getNormalizedMax(),
					DELTA);

		}

	}

	@Test
	public void testNormalizeDateLineCrossingWest() {

		final double minRange = -200;
		final double maxRange = -170;

		final int expectedBinCount = 2;

		final BinRange[] expectedBinRanges = new BinRange[] {
			new BinRange(
					-180,
					-170),
			new BinRange(
					160,
					180)
		};

		BinRange[] binRange = getNormalizedRanges(
				minRange,
				maxRange);

		Assert.assertEquals(
				expectedBinCount,
				binRange.length);

		for (int i = 0; i < binRange.length; i++) {

			Assert.assertEquals(
					expectedBinRanges[i].getNormalizedMin(),
					binRange[i].getNormalizedMin(),
					DELTA);

			Assert.assertEquals(
					expectedBinRanges[i].getNormalizedMax(),
					binRange[i].getNormalizedMax(),
					DELTA);

		}

	}

	private BinRange[] getNormalizedRanges(
			final double minRange,
			final double maxRange ) {

		return new LongitudeDefinition().getNormalizedRanges(new NumericRange(
				minRange,
				maxRange));

	}

	private double getNormalizedLongitudeValue(
			final double value ) {

		return LongitudeDefinition.getNormalizedLongitude(value);

	}

}
