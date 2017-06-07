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
package mil.nga.giat.geowave.core.geotime.index.sfc.hilbert;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.SignedBytes;

public class HilbertSFCTest
{

	@Test
	public void testGetId_2DSpatialMaxValue()
			throws Exception {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final double[] testValues = new double[] {
			90,
			180
		};
		final long expectedID = 3074457345618258602L;

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);
		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testGetId_2DSpatialMinValue()
			throws Exception {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final double[] testValues = new double[] {
			-90,
			-180
		};
		final long expectedID = 0L;

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testGetId_2DSpatialCentroidValue()
			throws Exception {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final double[] testValues = new double[] {
			0,
			0
		};
		final long expectedID = 768614336404564650L;

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);
		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testGetId_2DSpatialLexicographicOrdering()
			throws Exception {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final double[] minValue = new double[] {
			-90,
			-180
		};
		final double[] maxValue = new double[] {
			90,
			180
		};

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		Assert.assertTrue(SignedBytes.lexicographicalComparator().compare(
				hilbertSFC.getId(minValue),
				hilbertSFC.getId(maxValue)) < 0);

	}

	// @Test(expected = IllegalArgumentException.class)
	public void testGetId_2DSpatialIllegalArgument()
			throws Exception {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final double[] testValues = new double[] {
			-100,
			-180
		};
		final long expectedID = 0L;

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testDecomposeQuery_2DSpatialOneIndexFilter() {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS),
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		// Create a IndexRange object using the x axis
		final NumericRange rangeX = new NumericRange(
				55,
				57);

		// Create a IndexRange object using the y axis
		final NumericRange rangeY = new NumericRange(
				25,
				27);
		final BasicNumericDataset spatialQuery = new BasicNumericDataset(
				new NumericData[] {
					rangeX,
					rangeY
				});

		final RangeDecomposition rangeDecomposition = hilbertSFC.decomposeRange(
				spatialQuery,
				true,
				1);

		Assert.assertEquals(
				1,
				rangeDecomposition.getRanges().length);

	}

	@Test
	public void testDecomposeQuery_2DSpatialTwentyIndexFilters() {

		final int LATITUDE_BITS = 31;
		final int LONGITUDE_BITS = 31;

		final SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS),
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS)
		};

		final SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);
		// Create a IndexRange object using the x axis
		final NumericRange rangeX = new NumericRange(
				10,
				57);

		// Create a IndexRange object using the y axis
		final NumericRange rangeY = new NumericRange(
				25,
				50);
		final BasicNumericDataset spatialQuery = new BasicNumericDataset(
				new NumericData[] {
					rangeX,
					rangeY
				});

		final RangeDecomposition rangeDecomposition = hilbertSFC.decomposeRange(
				spatialQuery,
				true,
				20);

		Assert.assertEquals(
				20,
				rangeDecomposition.getRanges().length);

	}

	/* public void testDecomposeQuery_2DSpatialRanges() {} */
}
