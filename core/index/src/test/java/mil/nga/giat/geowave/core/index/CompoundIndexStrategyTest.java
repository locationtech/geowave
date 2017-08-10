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
package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

public class CompoundIndexStrategyTest
{

	private static final NumericDimensionDefinition[] DIMENSIONS = new NumericDimensionDefinition[] {
		new BasicDimensionDefinition(
				0,
				1000)
	};

	private static final NumericIndexStrategy simpleIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
			DIMENSIONS,
			new int[] {
				16
			},
			SFCType.HILBERT);

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
			simpleIndexStrategy,
			sfcIndexStrategy);

	private static final NumericRange dimension1Range = new NumericRange(
			2,
			4);
	private static final NumericRange dimension2Range = new NumericRange(
			50.0,
			50.025);
	private static final NumericRange dimension3Range = new NumericRange(
			-20.5,
			-20.455);
	private static final MultiDimensionalNumericData simpleIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension1Range,
			});
	private static final MultiDimensionalNumericData sfcIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension2Range,
				dimension3Range
			});
	private static final MultiDimensionalNumericData compoundIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension1Range,
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
	public void testNumberOfDimensionsPerIndexStrategy() {
		final int[] numDimensionsPerStrategy = compoundIndexStrategy.getNumberOfDimensionsPerIndexStrategy();
		Assert.assertEquals(
				1,
				numDimensionsPerStrategy[0]);
		Assert.assertEquals(
				2,
				numDimensionsPerStrategy[1]);
	}

	@Test
	public void testGetNumberOfDimensions() {
		final int numDimensions = compoundIndexStrategy.getNumberOfDimensions();
		Assert.assertEquals(
				3,
				numDimensions);
	}

	@Test
	public void testCompositionOfByteArrayId() {
		final ByteArrayId id1 = new ByteArrayId(
				"hello");
		final ByteArrayId id2 = new ByteArrayId(
				"world!!");
		final ByteArrayId compoundId = compoundIndexStrategy.composeByteArrayId(
				id1,
				id2);
		final ByteArrayId[] decomposedId = compoundIndexStrategy.decomposeByteArrayId(compoundId);
		Assert.assertArrayEquals(
				id1.getBytes(),
				decomposedId[0].getBytes());
		Assert.assertArrayEquals(
				id2.getBytes(),
				decomposedId[1].getBytes());
	}

	@Test
	public void testGetQueryRangesWithMaximumNumberOfRanges() {
		final List<ByteArrayRange> simpleIndexRanges = simpleIndexStrategy.getQueryRanges(simpleIndexedRange);
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (final ByteArrayRange r1 : simpleIndexRanges) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArrayId start = compoundIndexStrategy.composeByteArrayId(
						r1.getStart(),
						r2.getStart());
				final ByteArrayId end = compoundIndexStrategy.composeByteArrayId(
						r1.getEnd(),
						r2.getEnd());
				ranges.add(new ByteArrayRange(
						start,
						end));
			}
		}
		final Set<ByteArrayRange> testRanges = new HashSet<>(
				ranges);
		final Set<ByteArrayRange> compoundIndexRanges = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(compoundIndexedRange));
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

	@Test
	public void testGetQueryRanges() {
		final List<ByteArrayRange> simpleIndexRanges = simpleIndexStrategy.getQueryRanges(
				simpleIndexedRange,
				3);
		final int maxRangesStrategy2 = 8 / simpleIndexRanges.size();
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(
				sfcIndexedRange,
				maxRangesStrategy2);
		final List<ByteArrayRange> ranges = new ArrayList<>(
				simpleIndexRanges.size() * sfcIndexRanges.size());
		for (final ByteArrayRange r1 : simpleIndexRanges) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArrayId start = compoundIndexStrategy.composeByteArrayId(
						r1.getStart(),
						r2.getStart());
				final ByteArrayId end = compoundIndexStrategy.composeByteArrayId(
						r1.getEnd(),
						r2.getEnd());
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
						8));
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

	@Test
	public void testGetInsertionIds() {
		final List<ByteArrayId> ids = new ArrayList<>();
		final List<ByteArrayId> ids1 = simpleIndexStrategy.getInsertionIds(
				simpleIndexedRange,
				3);
		final int maxEstDuplicatesStrategy2 = 8 / ids1.size();
		final List<ByteArrayId> ids2 = sfcIndexStrategy.getInsertionIds(
				sfcIndexedRange,
				maxEstDuplicatesStrategy2);
		for (final ByteArrayId id1 : ids1) {
			for (final ByteArrayId id2 : ids2) {
				ids.add(compoundIndexStrategy.composeByteArrayId(
						id1,
						id2));
			}
		}
		final Set<ByteArrayId> testIds = new HashSet<>(
				ids);
		final Set<ByteArrayId> compoundIndexIds = new HashSet<>(
				compoundIndexStrategy.getInsertionIds(
						compoundIndexedRange,
						8));
		Assert.assertTrue(testIds.containsAll(compoundIndexIds));
		Assert.assertTrue(compoundIndexIds.containsAll(testIds));
	}

	@Test
	public void testGetCoordinatesPerDimension() {
		final ByteArrayId compoundIndexInsertionId = new ByteArrayId(
				new byte[] {
					16,
					0,
					-125,
					16,
					-46,
					-93,
					-110,
					-31,
					0,
					0,
					0,
					3
				});
		final ByteArrayId[] insertionIds = compoundIndexStrategy.decomposeByteArrayId(compoundIndexInsertionId);
		final MultiDimensionalCoordinates simpleIndexCoordinatesPerDim = simpleIndexStrategy
				.getCoordinatesPerDimension(insertionIds[0]);
		final MultiDimensionalCoordinates sfcIndexCoordinatesPerDim = sfcIndexStrategy
				.getCoordinatesPerDimension(insertionIds[1]);
		final MultiDimensionalCoordinates coordinatesPerDim = compoundIndexStrategy
				.getCoordinatesPerDimension(compoundIndexInsertionId);
		Assert.assertTrue(Long.compare(
				simpleIndexCoordinatesPerDim.getCoordinate(
						0).getCoordinate(),
				coordinatesPerDim.getCoordinate(
						0).getCoordinate()) == 0);
		Assert.assertTrue(Long.compare(
				sfcIndexCoordinatesPerDim.getCoordinate(
						0).getCoordinate(),
				coordinatesPerDim.getCoordinate(
						1).getCoordinate()) == 0);
		Assert.assertTrue(Long.compare(
				sfcIndexCoordinatesPerDim.getCoordinate(
						1).getCoordinate(),
				coordinatesPerDim.getCoordinate(
						2).getCoordinate()) == 0);
	}

	@Test
	public void testGetRangeForId() {
		final ByteArrayId compoundIndexInsertionId = new ByteArrayId(
				new byte[] {
					16,
					0,
					-125,
					16,
					-46,
					-93,
					-110,
					-31,
					0,
					0,
					0,
					3
				});
		final ByteArrayId[] insertionIds = compoundIndexStrategy.decomposeByteArrayId(compoundIndexInsertionId);
		final MultiDimensionalNumericData simpleIndexRange = simpleIndexStrategy.getRangeForId(insertionIds[0]);
		final MultiDimensionalNumericData sfcIndexRange = sfcIndexStrategy.getRangeForId(insertionIds[1]);
		final MultiDimensionalNumericData range = compoundIndexStrategy.getRangeForId(compoundIndexInsertionId);
		Assert.assertEquals(
				simpleIndexRange.getDimensionCount(),
				1);
		Assert.assertEquals(
				sfcIndexRange.getDimensionCount(),
				2);
		Assert.assertEquals(
				range.getDimensionCount(),
				3);
		Assert.assertTrue(Double.compare(
				simpleIndexRange.getMinValuesPerDimension()[0],
				range.getMinValuesPerDimension()[0]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMinValuesPerDimension()[0],
				range.getMinValuesPerDimension()[1]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMinValuesPerDimension()[1],
				range.getMinValuesPerDimension()[2]) == 0);
		Assert.assertTrue(Double.compare(
				simpleIndexRange.getMaxValuesPerDimension()[0],
				range.getMaxValuesPerDimension()[0]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMaxValuesPerDimension()[0],
				range.getMaxValuesPerDimension()[1]) == 0);
		Assert.assertTrue(Double.compare(
				sfcIndexRange.getMaxValuesPerDimension()[1],
				range.getMaxValuesPerDimension()[2]) == 0);
	}

	@Test
	public void testGetHighestPrecisionIdRangePerDimension() {
		final double[] simpleIndexPrecision = simpleIndexStrategy.getHighestPrecisionIdRangePerDimension();
		final double[] sfcIndexPrecision = sfcIndexStrategy.getHighestPrecisionIdRangePerDimension();
		final double[] precisionPerDim = compoundIndexStrategy.getHighestPrecisionIdRangePerDimension();
		Assert.assertTrue(Double.compare(
				precisionPerDim[0],
				simpleIndexPrecision[0]) == 0);
		Assert.assertTrue(Double.compare(
				precisionPerDim[1],
				sfcIndexPrecision[0]) == 0);
		Assert.assertTrue(Double.compare(
				precisionPerDim[2],
				sfcIndexPrecision[1]) == 0);
	}

	@Test
	public void testHints() {
		final List<ByteArrayId> ids = compoundIndexStrategy.getInsertionIds(
				compoundIndexedRange,
				8);

		List<IndexMetaData> metaData = compoundIndexStrategy.createMetaData();
		for (IndexMetaData imd : metaData) {
			imd.insertionIdsAdded(ids);
		}

		final List<ByteArrayRange> simpleIndexRanges = simpleIndexStrategy.getQueryRanges(simpleIndexedRange);
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (final ByteArrayRange r1 : simpleIndexRanges) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArrayId start = compoundIndexStrategy.composeByteArrayId(
						r1.getStart(),
						r2.getStart());
				final ByteArrayId end = compoundIndexStrategy.composeByteArrayId(
						r1.getEnd(),
						r2.getEnd());
				ranges.add(new ByteArrayRange(
						start,
						end));
			}
		}

		final Set<ByteArrayRange> compoundIndexRangesWithoutHints = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(compoundIndexedRange));
		final Set<ByteArrayRange> compoundIndexRangesWithHints = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						compoundIndexedRange,
						metaData.toArray(new IndexMetaData[metaData.size()])));
		Assert.assertTrue(compoundIndexRangesWithoutHints.containsAll(compoundIndexRangesWithHints));
		Assert.assertTrue(compoundIndexRangesWithHints.containsAll(compoundIndexRangesWithoutHints));

		List<Persistable> newMetaData = PersistenceUtils.fromBinaryAsList(PersistenceUtils.toBinary(metaData));
		final Set<ByteArrayRange> compoundIndexRangesWithHints2 = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						compoundIndexedRange,
						metaData.toArray(new IndexMetaData[newMetaData.size()])));
		Assert.assertTrue(compoundIndexRangesWithoutHints.containsAll(compoundIndexRangesWithHints2));
		Assert.assertTrue(compoundIndexRangesWithHints2.containsAll(compoundIndexRangesWithoutHints));

	}
}
