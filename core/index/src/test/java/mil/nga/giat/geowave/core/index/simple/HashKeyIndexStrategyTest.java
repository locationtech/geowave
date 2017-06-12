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
package mil.nga.giat.geowave.core.index.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

import org.junit.Assert;
import org.junit.Test;

public class HashKeyIndexStrategyTest
{

	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new BasicDimensionDefinition(
				-180,
				180),
		new BasicDimensionDefinition(
				-90,
				90)
	};

	private static final NumericIndexStrategy sfcIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_DIMENSIONS,
			new int[] {
				16,
				16
			},
			SFCType.HILBERT);

	private static final HashKeyIndexStrategy hashIdexStrategy = new HashKeyIndexStrategy(
			3);
	private static final CompoundIndexStrategy compoundIndexStrategy = new CompoundIndexStrategy(
			hashIdexStrategy,
			sfcIndexStrategy);

	private static final NumericRange dimension1Range = new NumericRange(
			50.0,
			50.025);
	private static final NumericRange dimension2Range = new NumericRange(
			-20.5,
			-20.455);
	private static final MultiDimensionalNumericData sfcIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension1Range,
				dimension2Range
			});

	@Test
	public void testDistribution() {
		final Map<ByteArrayId, Long> counts = new HashMap<ByteArrayId, Long>();
		int total = 0;
		for (double x = 90; x < 180; x += 0.05) {
			for (double y = 50; y < 90; y += 0.5) {
				final NumericRange dimension1Range = new NumericRange(
						x,
						x + 0.002);
				final NumericRange dimension2Range = new NumericRange(
						y - 0.002,
						y);
				final MultiDimensionalNumericData sfcIndexedRange = new BasicNumericDataset(
						new NumericData[] {
							dimension1Range,
							dimension2Range
						});
				for (ByteArrayId id : hashIdexStrategy.getInsertionIds(sfcIndexedRange)) {
					Long count = counts.get(id);
					long nextcount = count == null ? 1 : count + 1;
					counts.put(
							id,
							nextcount);
					total++;
				}
			}
		}

		double mean = total / counts.size();
		double diff = 0.0;
		for (Long count : counts.values()) {
			diff += Math.pow(
					mean - count,
					2);
		}
		double sd = Math.sqrt(diff / counts.size());
		assertTrue(sd < mean * 0.18);
	}

	@Test
	public void testBinaryEncoding() {
		final byte[] bytes = PersistenceUtils.toBinary(compoundIndexStrategy);
		final CompoundIndexStrategy deserializedStrategy = (CompoundIndexStrategy) PersistenceUtils.fromBinary(bytes);
		final byte[] bytes2 = PersistenceUtils.toBinary(deserializedStrategy);
		Assert.assertArrayEquals(
				bytes,
				bytes2);
	}

	@Test
	public void testNumberOfDimensionsPerIndexStrategy() {
		final int[] numDimensionsPerStrategy = compoundIndexStrategy.getNumberOfDimensionsPerIndexStrategy();
		Assert.assertEquals(
				0,
				numDimensionsPerStrategy[0]);
		Assert.assertEquals(
				2,
				numDimensionsPerStrategy[1]);
	}

	@Test
	public void testGetNumberOfDimensions() {
		final int numDimensions = compoundIndexStrategy.getNumberOfDimensions();
		Assert.assertEquals(
				2,
				numDimensions);
	}

	@Test
	public void testGetCoordinatesPerDimension() {

		final NumericRange dimension1Range = new NumericRange(
				20.01,
				20.02);
		final NumericRange dimension2Range = new NumericRange(
				30.51,
				30.59);
		final MultiDimensionalNumericData sfcIndexedRange = new BasicNumericDataset(
				new NumericData[] {
					dimension1Range,
					dimension2Range
				});
		for (ByteArrayId id : compoundIndexStrategy.getInsertionIds(sfcIndexedRange)) {
			MultiDimensionalCoordinates coords = compoundIndexStrategy.getCoordinatesPerDimension(id);
			assertTrue(coords.getCoordinate(
					0).getCoordinate() > 0);
			assertTrue(coords.getCoordinate(
					1).getCoordinate() > 0);
			MultiDimensionalNumericData nd = compoundIndexStrategy.getRangeForId(id);
			assertEquals(
					20.02,
					nd.getMaxValuesPerDimension()[0],
					0.1);
			assertEquals(
					30.59,
					nd.getMaxValuesPerDimension()[1],
					0.2);
			assertEquals(
					20.01,
					nd.getMinValuesPerDimension()[0],
					0.1);
			assertEquals(
					30.57,
					nd.getMinValuesPerDimension()[1],
					0.2);
		}
	}

	@Test
	public void testGetQueryRangesWithMaximumNumberOfRanges() {
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArrayId start = compoundIndexStrategy.composeByteArrayId(
						new ByteArrayId(
								new byte[] {
									(byte) i
								}),
						r2.getStart());
				final ByteArrayId end = compoundIndexStrategy.composeByteArrayId(
						new ByteArrayId(
								new byte[] {
									(byte) i
								}),
						r2.getEnd());
				ranges.add(new ByteArrayRange(
						start,
						end));
			}
		}
		final Set<ByteArrayRange> testRanges = new HashSet<>(
				ranges);
		final Set<ByteArrayRange> compoundIndexRanges = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(sfcIndexedRange));
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

}
