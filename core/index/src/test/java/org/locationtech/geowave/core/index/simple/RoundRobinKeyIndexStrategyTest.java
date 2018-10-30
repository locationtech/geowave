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
package org.locationtech.geowave.core.index.simple;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.CompoundIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.simple.RoundRobinKeyIndexStrategy;

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
	public void testGetNumberOfDimensions() {
		final int numDimensions = compoundIndexStrategy.getNumberOfDimensions();
		Assert.assertEquals(
				2,
				numDimensions);
	}

	@Test
	public void testGetQueryRangesWithMaximumNumberOfRanges() {
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(
				sfcIndexedRange).getCompositeQueryRanges();
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArray start = new ByteArray(
						ByteArrayUtils.combineArrays(
								new byte[] {
									(byte) i
								},
								r2.getStart().getBytes()));
				final ByteArray end = new ByteArray(
						ByteArrayUtils.combineArrays(
								new byte[] {
									(byte) i
								},
								r2.getEnd().getBytes()));
				ranges.add(new ByteArrayRange(
						start,
						end));
			}
		}
		final Set<ByteArrayRange> testRanges = new HashSet<>(
				ranges);
		final Set<ByteArrayRange> compoundIndexRanges = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						sfcIndexedRange).getCompositeQueryRanges());
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

	@Test
	public void testUniformityAndLargeKeySet() {
		final RoundRobinKeyIndexStrategy strategy = new RoundRobinKeyIndexStrategy(
				512);
		final Map<ByteArray, Integer> countMap = new HashMap<ByteArray, Integer>();
		for (int i = 0; i < 2048; i++) {
			final Set<ByteArray> ids = strategy.getInsertionPartitionKeys(sfcIndexedRange);
			assertEquals(
					1,
					ids.size());
			final ByteArray key = ids.iterator().next();
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
		final List<ByteArray> ids = new ArrayList<>();

		final InsertionIds ids2 = sfcIndexStrategy.getInsertionIds(
				sfcIndexedRange,
				1);
		final List<ByteArray> compositeIds = ids2.getCompositeInsertionIds();
		for (int i = 0; i < 3; i++) {
			for (final ByteArray id2 : compositeIds) {
				ids.add(new ByteArray(
						ByteArrayUtils.combineArrays(
								new byte[] {
									(byte) i
								},
								id2.getBytes())));
			}
		}
		final Set<ByteArray> testIds = new HashSet<>(
				ids);
		final Set<ByteArray> compoundIndexIds = new HashSet<>(
				compoundIndexStrategy.getInsertionIds(
						sfcIndexedRange,
						8).getCompositeInsertionIds());
		Assert.assertTrue(testIds.containsAll(compoundIndexIds));
		final SinglePartitionInsertionIds id2 = ids2.getPartitionKeys().iterator().next();
		final MultiDimensionalCoordinates sfcIndexCoordinatesPerDim = sfcIndexStrategy.getCoordinatesPerDimension(
				id2.getPartitionKey(),
				id2.getSortKeys().get(
						0));
		// the first 2 bytes are the partition keys
		final MultiDimensionalCoordinates coordinatesPerDim = compoundIndexStrategy.getCoordinatesPerDimension(
				new ByteArray(
						Arrays.copyOfRange(
								ids.get(
										0).getBytes(),
								0,
								2)),
				new ByteArray(
						Arrays.copyOfRange(
								ids.get(
										0).getBytes(),
								2,
								ids.get(
										0).getBytes().length)));

		Assert.assertTrue(sfcIndexCoordinatesPerDim.equals(coordinatesPerDim));
	}

}
