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

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.hilbert.PrimitiveHilbertSFCOperations;
import mil.nga.giat.geowave.core.index.sfc.hilbert.UnboundedHilbertSFCOperations;

import org.junit.Assert;
import org.junit.Test;

import com.google.uzaygezen.core.CompactHilbertCurve;
import com.google.uzaygezen.core.MultiDimensionalSpec;

public class PrimitiveHilbertSFCTest
{
	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(
				true)
	};

	@Test
	public void testSpatialGetIdAndQueryDecomposition62BitsTotal() {
		final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[SPATIAL_DIMENSIONS.length];
		int totalPrecision = 0;
		final List<Integer> bitsPerDimension = new ArrayList<Integer>();
		for (int d = 0; d < SPATIAL_DIMENSIONS.length; d++) {
			final int bitsOfPrecision = 31;
			sfcDimensions[d] = new SFCDimensionDefinition(
					SPATIAL_DIMENSIONS[d],
					bitsOfPrecision);

			bitsPerDimension.add(bitsOfPrecision);
			totalPrecision += bitsOfPrecision;
		}

		final CompactHilbertCurve compactHilbertCurve = new CompactHilbertCurve(
				new MultiDimensionalSpec(
						bitsPerDimension));
		final PrimitiveHilbertSFCOperations testOperations = new PrimitiveHilbertSFCOperations();

		// assume the unbounded SFC is the true results, regardless they should
		// both produce the same results
		final UnboundedHilbertSFCOperations expectedResultOperations = new UnboundedHilbertSFCOperations();
		testOperations.init(sfcDimensions);
		expectedResultOperations.init(sfcDimensions);
		final double[] testValues1 = new double[] {
			45,
			45
		};

		final double[] testValues2 = new double[] {
			0,
			0
		};
		final double[] testValues3 = new double[] {
			-1.235456,
			-67.9213546
		};
		final double[] testValues4 = new double[] {
			-61.2354561024897435868943753568436598645436,
			42.921354693742875894356895549054690704378590896
		};
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues1,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues1,
						compactHilbertCurve,
						sfcDimensions));
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues2,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues2,
						compactHilbertCurve,
						sfcDimensions));
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues3,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues3,
						compactHilbertCurve,
						sfcDimensions));
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues4,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues4,
						compactHilbertCurve,
						sfcDimensions));
		final NumericRange rangeLongitude1 = new NumericRange(
				0,
				1);

		final NumericRange rangeLatitude1 = new NumericRange(
				0,
				1);
		final NumericRange rangeLongitude2 = new NumericRange(
				-21.324967549,
				28.4285637846834432543);

		final NumericRange rangeLatitude2 = new NumericRange(
				-43.7894445665435346547657867847657654,
				32.3254325834896543657895436543543659);

		final NumericRange rangeLongitude3 = new NumericRange(
				-10,
				0);

		final NumericRange rangeLatitude3 = new NumericRange(
				-10,
				0);
		final NumericRange rangeLongitude4 = new NumericRange(
				-Double.MIN_VALUE,
				0);

		final NumericRange rangeLatitude4 = new NumericRange(
				0,
				Double.MIN_VALUE);
		final RangeDecomposition expectedResult1 = expectedResultOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude1,
					rangeLatitude1
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				true);
		final RangeDecomposition testResult1 = testOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude1,
					rangeLatitude1
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				true);
		Assert.assertTrue(expectedResult1.getRanges().length == testResult1.getRanges().length);
		for (int i = 0; i < expectedResult1.getRanges().length; i++) {
			Assert.assertTrue(expectedResult1.getRanges()[i].equals(testResult1.getRanges()[i]));
		}
		final RangeDecomposition expectedResult2 = expectedResultOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude2,
					rangeLatitude2
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				true);
		final RangeDecomposition testResult2 = testOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude2,
					rangeLatitude2
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				true);
		Assert.assertTrue(expectedResult2.getRanges().length == testResult2.getRanges().length);
		for (int i = 0; i < expectedResult2.getRanges().length; i++) {
			Assert.assertTrue(expectedResult2.getRanges()[i].equals(testResult2.getRanges()[i]));
		}
		final RangeDecomposition expectedResult3 = expectedResultOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude3,
					rangeLatitude3
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				false);
		final RangeDecomposition testResult3 = testOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude3,
					rangeLatitude3
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				false);
		Assert.assertTrue(expectedResult3.getRanges().length == testResult3.getRanges().length);
		for (int i = 0; i < expectedResult3.getRanges().length; i++) {
			Assert.assertTrue(expectedResult3.getRanges()[i].equals(testResult3.getRanges()[i]));
		}
		final RangeDecomposition expectedResult4 = expectedResultOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude4,
					rangeLatitude4
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				false);
		final RangeDecomposition testResult4 = testOperations.decomposeRange(
				new NumericData[] {
					rangeLongitude4,
					rangeLatitude4
				},
				compactHilbertCurve,
				sfcDimensions,
				totalPrecision,
				Integer.MAX_VALUE,
				true,
				false);
		Assert.assertTrue(expectedResult4.getRanges().length == testResult4.getRanges().length);
		for (int i = 0; i < expectedResult4.getRanges().length; i++) {
			Assert.assertTrue(expectedResult4.getRanges()[i].equals(testResult4.getRanges()[i]));
		}
	}

	@Test
	public void testGetId48BitsPerDimension() {
		final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[20];

		final List<Integer> bitsPerDimension = new ArrayList<Integer>();
		for (int d = 0; d < sfcDimensions.length; d++) {
			final int bitsOfPrecision = 48;
			sfcDimensions[d] = new SFCDimensionDefinition(
					new BasicDimensionDefinition(
							0,
							1),
					bitsOfPrecision);

			bitsPerDimension.add(bitsOfPrecision);
		}

		final CompactHilbertCurve compactHilbertCurve = new CompactHilbertCurve(
				new MultiDimensionalSpec(
						bitsPerDimension));
		final PrimitiveHilbertSFCOperations testOperations = new PrimitiveHilbertSFCOperations();

		// assume the unbounded SFC is the true results, regardless they should
		// both produce the same results
		final UnboundedHilbertSFCOperations expectedResultOperations = new UnboundedHilbertSFCOperations();
		testOperations.init(sfcDimensions);
		expectedResultOperations.init(sfcDimensions);
		final double[] testValues1 = new double[] {
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
			Double.MIN_VALUE,
		};

		final double[] testValues2 = new double[] {
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0
		};
		final double[] testValues3 = new double[] {
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1,
			1
		};
		final double[] testValues4 = new double[] {
			0.2354561024897435868943753568436598645436,
			0.921354693742875894657658678436546547657867869789780790890789356895549054690704378590896,
			0.84754363905364783265784365843,
			0.7896543436756437856046562640234,
			0.3216819204957436913249032618969653,
			0.327219038596576238101046563945864390685476054,
			0.12189368934632894658343655436546754754665875784375308678932689368432,
			0.000327489326493291328326493457437584375043,
			0.3486563289543,
			0.96896758943758,
			0.98999897899879789789789789789789789789689,
			0.1275785478325478265925864359,
			0.124334325346554654,
			0.1234565,
			0.9876543,
			0.76634328932,
			0.64352843,
			0.5432342321,
			0.457686789,
			0.2046543435
		};
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues1,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues1,
						compactHilbertCurve,
						sfcDimensions));
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues2,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues2,
						compactHilbertCurve,
						sfcDimensions));
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues3,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues3,
						compactHilbertCurve,
						sfcDimensions));
		Assert.assertArrayEquals(
				expectedResultOperations.convertToHilbert(
						testValues4,
						compactHilbertCurve,
						sfcDimensions),
				testOperations.convertToHilbert(
						testValues4,
						compactHilbertCurve,
						sfcDimensions));

	}
}
