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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

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

public class RoundRobinKeyIndexStrategyTest
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

	private static final CompoundIndexStrategy compoundIndexStrategy = new CompoundIndexStrategy(
			new RoundRobinKeyIndexStrategy(),
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

	@Test
	public void testUniformityAndLargeKeySet() {
		final RoundRobinKeyIndexStrategy strategy = new RoundRobinKeyIndexStrategy(
				512);
		final Map<ByteArrayId, Integer> countMap = new HashMap<ByteArrayId, Integer>();
		for (int i = 0; i < 2048; i++) {
			final List<ByteArrayId> ids = strategy.getInsertionIds(sfcIndexedRange);
			assertEquals(
					1,
					ids.size());
			final ByteArrayId key = ids.get(0);
			if (countMap.containsKey(key)) {
				countMap.put(
						key,
						countMap.get(key) + 1);
			}
			else {
				countMap.put(
						key,
						1);
			}

		}
		for (final Integer i : countMap.values()) {
			assertEquals(
					4,
					i.intValue());
		}
	}

	@Test
	public void testGetInsertionIds() {
		final List<ByteArrayId> ids = new ArrayList<>();

		final List<ByteArrayId> ids2 = sfcIndexStrategy.getInsertionIds(
				sfcIndexedRange,
				1);
		for (int i = 0; i < 3; i++) {
			for (final ByteArrayId id2 : ids2) {
				ids.add(compoundIndexStrategy.composeByteArrayId(
						new ByteArrayId(
								new byte[] {
									(byte) i
								}),
						id2));
			}
		}
		final Set<ByteArrayId> testIds = new HashSet<>(
				ids);
		final Set<ByteArrayId> compoundIndexIds = new HashSet<>(
				compoundIndexStrategy.getInsertionIds(
						sfcIndexedRange,
						8));
		Assert.assertTrue(testIds.containsAll(compoundIndexIds));

		final MultiDimensionalCoordinates sfcIndexCoordinatesPerDim = sfcIndexStrategy.getCoordinatesPerDimension(ids2
				.get(0));
		final MultiDimensionalCoordinates coordinatesPerDim = compoundIndexStrategy.getCoordinatesPerDimension(ids
				.get(0));

		Assert.assertTrue(sfcIndexCoordinatesPerDim.equals(coordinatesPerDim));
	}

}
