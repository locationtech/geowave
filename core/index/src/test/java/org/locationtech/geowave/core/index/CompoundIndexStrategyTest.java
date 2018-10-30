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
package org.locationtech.geowave.core.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.CompoundIndexStrategy;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.PartitionIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.simple.HashKeyIndexStrategy;

public class CompoundIndexStrategyTest
{

	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new BasicDimensionDefinition(
				-180,
				180),
		new BasicDimensionDefinition(
				-90,
				90)
	};
	private static final PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> simpleIndexStrategy = new HashKeyIndexStrategy(
			10);
	private static final NumericIndexStrategy sfcIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_DIMENSIONS,
			new int[] {
				16,
				16
			},
			SFCType.HILBERT);

	private static final CompoundIndexStrategy compoundIndexStrategy = new CompoundIndexStrategy(
			simpleIndexStrategy,
			sfcIndexStrategy);

	private static final NumericRange dimension2Range = new NumericRange(
			50.0,
			50.025);
	private static final NumericRange dimension3Range = new NumericRange(
			-20.5,
			-20.455);
	private static final MultiDimensionalNumericData sfcIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension2Range,
				dimension3Range
			});
	private static final MultiDimensionalNumericData compoundIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension2Range,
				dimension3Range
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
		final Set<ByteArray> partitions = simpleIndexStrategy.getQueryPartitionKeys(sfcIndexedRange);
		final QueryRanges sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (final ByteArray r1 : partitions) {
			for (final ByteArrayRange r2 : sfcIndexRanges.getCompositeQueryRanges()) {
				final ByteArray start = new ByteArray(
						ByteArrayUtils.combineArrays(
								r1.getBytes(),
								r2.getStart().getBytes()));
				final ByteArray end = new ByteArray(
						ByteArrayUtils.combineArrays(
								r1.getBytes(),
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
						compoundIndexedRange).getCompositeQueryRanges());
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

	@Test
	public void testGetQueryRanges() {
		final Set<ByteArray> simpleIndexRanges = simpleIndexStrategy.getQueryPartitionKeys(sfcIndexedRange);
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(
				sfcIndexedRange,
				8).getCompositeQueryRanges();
		final List<ByteArrayRange> ranges = new ArrayList<>(
				simpleIndexRanges.size() * sfcIndexRanges.size());
		for (final ByteArray r1 : simpleIndexRanges) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArray start = new ByteArray(
						ByteArrayUtils.combineArrays(
								r1.getBytes(),
								r2.getStart().getBytes()));
				final ByteArray end = new ByteArray(
						ByteArrayUtils.combineArrays(
								r1.getBytes(),
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
						compoundIndexedRange,
						8).getCompositeQueryRanges());
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

	@Test
	public void testGetInsertionIds() {
		final List<ByteArray> ids = new ArrayList<>();
		final Set<ByteArray> ids1 = simpleIndexStrategy.getInsertionPartitionKeys(sfcIndexedRange);
		final int maxEstDuplicatesStrategy2 = 8 / ids1.size();
		final List<ByteArray> ids2 = sfcIndexStrategy.getInsertionIds(
				sfcIndexedRange,
				maxEstDuplicatesStrategy2).getCompositeInsertionIds();
		for (final ByteArray id1 : ids1) {
			for (final ByteArray id2 : ids2) {
				ids.add(new ByteArray(
						ByteArrayUtils.combineArrays(
								id1.getBytes(),
								id2.getBytes())));
			}
		}
		final Set<ByteArray> testIds = new HashSet<>(
				ids);
		final Set<ByteArray> compoundIndexIds = new HashSet<>(
				compoundIndexStrategy.getInsertionIds(
						compoundIndexedRange,
						8).getCompositeInsertionIds());
		Assert.assertTrue(testIds.containsAll(compoundIndexIds));
		Assert.assertTrue(compoundIndexIds.containsAll(testIds));
	}

	@Test
	public void testGetCoordinatesPerDimension() {

		final ByteArray compoundIndexPartitionKey = new ByteArray(
				new byte[] {
					16
				});
		final ByteArray compoundIndexSortKey = new ByteArray(
				new byte[] {
					-46,
					-93,
					-110,
					-31
				});
		final MultiDimensionalCoordinates sfcIndexCoordinatesPerDim = sfcIndexStrategy.getCoordinatesPerDimension(
				compoundIndexPartitionKey,
				compoundIndexSortKey);
		final MultiDimensionalCoordinates coordinatesPerDim = compoundIndexStrategy.getCoordinatesPerDimension(
				compoundIndexPartitionKey,
				compoundIndexSortKey);
		Assert.assertTrue(Long.compare(
				sfcIndexCoordinatesPerDim.getCoordinate(
						0).getCoordinate(),
				coordinatesPerDim.getCoordinate(
						0).getCoordinate()) == 0);
		Assert.assertTrue(Long.compare(
				sfcIndexCoordinatesPerDim.getCoordinate(
						1).getCoordinate(),
				coordinatesPerDim.getCoordinate(
						1).getCoordinate()) == 0);
	}

	@Test
	public void testGetRangeForId() {
		final ByteArray sfcIndexPartitionKey = new ByteArray(
				new byte[] {
					16
				});
		final ByteArray sfcIndexSortKey = new ByteArray(
				new byte[] {
					-46,
					-93,
					-110,
					-31
				});
		final MultiDimensionalNumericData sfcIndexRange = sfcIndexStrategy.getRangeForId(
				sfcIndexPartitionKey,
				sfcIndexSortKey);
		final MultiDimensionalNumericData range = compoundIndexStrategy.getRangeForId(
				sfcIndexPartitionKey,
				sfcIndexSortKey);
		Assert.assertEquals(
				sfcIndexRange.getDimensionCount(),
				2);
		Assert.assertEquals(
				range.getDimensionCount(),
				2);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMinValuesPerDimension()[0],
				range.getMinValuesPerDimension()[0]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMinValuesPerDimension()[1],
				range.getMinValuesPerDimension()[1]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMaxValuesPerDimension()[0],
				range.getMaxValuesPerDimension()[0]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMaxValuesPerDimension()[1],
				range.getMaxValuesPerDimension()[1]) == 0);
	}

	@Test
	public void testHints() {
		final InsertionIds ids = compoundIndexStrategy.getInsertionIds(
				compoundIndexedRange,
				8);

		final List<IndexMetaData> metaData = compoundIndexStrategy.createMetaData();
		for (final IndexMetaData imd : metaData) {
			imd.insertionIdsAdded(ids);
		}

		final Set<ByteArray> simpleIndexRanges = simpleIndexStrategy.getQueryPartitionKeys(sfcIndexedRange);
		final QueryRanges sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (final ByteArray r1 : simpleIndexRanges) {
			for (final ByteArrayRange r2 : sfcIndexRanges.getCompositeQueryRanges()) {
				final ByteArray start = new ByteArray(
						ByteArrayUtils.combineArrays(
								r1.getBytes(),
								r2.getStart().getBytes()));
				final ByteArray end = new ByteArray(
						ByteArrayUtils.combineArrays(
								r1.getBytes(),
								r2.getEnd().getBytes()));
				ranges.add(new ByteArrayRange(
						start,
						end));
			}
		}

		final Set<ByteArrayRange> compoundIndexRangesWithoutHints = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						compoundIndexedRange).getCompositeQueryRanges());
		final Set<ByteArrayRange> compoundIndexRangesWithHints = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						compoundIndexedRange,
						metaData.toArray(new IndexMetaData[metaData.size()])).getCompositeQueryRanges());
		Assert.assertTrue(compoundIndexRangesWithoutHints.containsAll(compoundIndexRangesWithHints));
		Assert.assertTrue(compoundIndexRangesWithHints.containsAll(compoundIndexRangesWithoutHints));

		List<Persistable> newMetaData = PersistenceUtils.fromBinaryAsList(PersistenceUtils.toBinary(metaData));
		final Set<ByteArrayRange> compoundIndexRangesWithHints2 = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						compoundIndexedRange,
						metaData.toArray(new IndexMetaData[newMetaData.size()])).getCompositeQueryRanges());
		Assert.assertTrue(compoundIndexRangesWithoutHints.containsAll(compoundIndexRangesWithHints2));
		Assert.assertTrue(compoundIndexRangesWithHints2.containsAll(compoundIndexRangesWithoutHints));

	}
}
