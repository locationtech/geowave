/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.simple.HashKeyIndexStrategy;

public class CompoundIndexStrategyTest {

  private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS =
      new NumericDimensionDefinition[] {
          new BasicDimensionDefinition(-180, 180),
          new BasicDimensionDefinition(-90, 90)};
  private static final PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> simpleIndexStrategy =
      new HashKeyIndexStrategy(10);
  private static final NumericIndexStrategy sfcIndexStrategy =
      TieredSFCIndexFactory.createSingleTierStrategy(
          SPATIAL_DIMENSIONS,
          new int[] {16, 16},
          SFCType.HILBERT);

  private static final CompoundIndexStrategy compoundIndexStrategy =
      new CompoundIndexStrategy(simpleIndexStrategy, sfcIndexStrategy);

  private static final NumericRange dimension2Range = new NumericRange(50.0, 50.025);
  private static final NumericRange dimension3Range = new NumericRange(-20.5, -20.455);
  private static final MultiDimensionalNumericData sfcIndexedRange =
      new BasicNumericDataset(new NumericData[] {dimension2Range, dimension3Range});
  private static final MultiDimensionalNumericData compoundIndexedRange =
      new BasicNumericDataset(new NumericData[] {dimension2Range, dimension3Range});

  @Test
  public void testBinaryEncoding() {
    final byte[] bytes = PersistenceUtils.toBinary(compoundIndexStrategy);
    final CompoundIndexStrategy deserializedStrategy =
        (CompoundIndexStrategy) PersistenceUtils.fromBinary(bytes);
    final byte[] bytes2 = PersistenceUtils.toBinary(deserializedStrategy);
    Assert.assertArrayEquals(bytes, bytes2);
  }

  @Test
  public void testGetNumberOfDimensions() {
    final int numDimensions = compoundIndexStrategy.getNumberOfDimensions();
    Assert.assertEquals(2, numDimensions);
  }

  @Test
  public void testGetQueryRangesWithMaximumNumberOfRanges() {
    final byte[][] partitions = simpleIndexStrategy.getQueryPartitionKeys(sfcIndexedRange);
    final QueryRanges sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
    final List<ByteArrayRange> ranges = new ArrayList<>();
    for (final byte[] r1 : partitions) {
      for (final ByteArrayRange r2 : sfcIndexRanges.getCompositeQueryRanges()) {
        final byte[] start = ByteArrayUtils.combineArrays(r1, r2.getStart());
        final byte[] end = ByteArrayUtils.combineArrays(r1, r2.getEnd());
        ranges.add(new ByteArrayRange(start, end));
      }
    }
    final Set<ByteArrayRange> testRanges = new HashSet<>(ranges);
    final Set<ByteArrayRange> compoundIndexRanges =
        new HashSet<>(
            compoundIndexStrategy.getQueryRanges(compoundIndexedRange).getCompositeQueryRanges());
    Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
    Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
  }

  @Test
  public void testGetQueryRanges() {
    final byte[][] simpleIndexRanges = simpleIndexStrategy.getQueryPartitionKeys(sfcIndexedRange);
    final List<ByteArrayRange> sfcIndexRanges =
        sfcIndexStrategy.getQueryRanges(sfcIndexedRange, 8).getCompositeQueryRanges();
    final List<ByteArrayRange> ranges =
        new ArrayList<>(simpleIndexRanges.length * sfcIndexRanges.size());
    for (final byte[] r1 : simpleIndexRanges) {
      for (final ByteArrayRange r2 : sfcIndexRanges) {
        final byte[] start = ByteArrayUtils.combineArrays(r1, r2.getStart());
        final byte[] end = ByteArrayUtils.combineArrays(r1, r2.getEnd());
        ranges.add(new ByteArrayRange(start, end));
      }
    }
    final Set<ByteArrayRange> testRanges = new HashSet<>(ranges);
    final Set<ByteArrayRange> compoundIndexRanges =
        new HashSet<>(
            compoundIndexStrategy.getQueryRanges(
                compoundIndexedRange,
                8).getCompositeQueryRanges());
    Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
    Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
  }

  @Test
  public void testGetInsertionIds() {
    final List<byte[]> ids = new ArrayList<>();
    final byte[][] ids1 = simpleIndexStrategy.getInsertionPartitionKeys(sfcIndexedRange);
    final int maxEstDuplicatesStrategy2 = 8 / ids1.length;
    final List<byte[]> ids2 =
        sfcIndexStrategy.getInsertionIds(
            sfcIndexedRange,
            maxEstDuplicatesStrategy2).getCompositeInsertionIds();
    for (final byte[] id1 : ids1) {
      for (final byte[] id2 : ids2) {
        ids.add(ByteArrayUtils.combineArrays(id1, id2));
      }
    }
    final Set<ByteArray> testIds =
        new HashSet<>(ids.stream().map(i -> new ByteArray(i)).collect(Collectors.toList()));
    final Set<ByteArray> compoundIndexIds =
        new HashSet<>(
            compoundIndexStrategy.getInsertionIds(
                compoundIndexedRange,
                8).getCompositeInsertionIds().stream().map(i -> new ByteArray(i)).collect(
                    Collectors.toList()));
    Assert.assertTrue(testIds.containsAll(compoundIndexIds));
    Assert.assertTrue(compoundIndexIds.containsAll(testIds));
  }

  @Test
  public void testGetCoordinatesPerDimension() {

    final byte[] compoundIndexPartitionKey = new byte[] {16};
    final byte[] compoundIndexSortKey = new byte[] {-46, -93, -110, -31};
    final MultiDimensionalCoordinates sfcIndexCoordinatesPerDim =
        sfcIndexStrategy.getCoordinatesPerDimension(
            compoundIndexPartitionKey,
            compoundIndexSortKey);
    final MultiDimensionalCoordinates coordinatesPerDim =
        compoundIndexStrategy.getCoordinatesPerDimension(
            compoundIndexPartitionKey,
            compoundIndexSortKey);
    Assert.assertTrue(
        Long.compare(
            sfcIndexCoordinatesPerDim.getCoordinate(0).getCoordinate(),
            coordinatesPerDim.getCoordinate(0).getCoordinate()) == 0);
    Assert.assertTrue(
        Long.compare(
            sfcIndexCoordinatesPerDim.getCoordinate(1).getCoordinate(),
            coordinatesPerDim.getCoordinate(1).getCoordinate()) == 0);
  }

  @Test
  public void testGetRangeForId() {
    final byte[] sfcIndexPartitionKey = new byte[] {16};
    final byte[] sfcIndexSortKey = new byte[] {-46, -93, -110, -31};
    final MultiDimensionalNumericData sfcIndexRange =
        sfcIndexStrategy.getRangeForId(sfcIndexPartitionKey, sfcIndexSortKey);
    final MultiDimensionalNumericData range =
        compoundIndexStrategy.getRangeForId(sfcIndexPartitionKey, sfcIndexSortKey);
    Assert.assertEquals(sfcIndexRange.getDimensionCount(), 2);
    Assert.assertEquals(range.getDimensionCount(), 2);
    Assert.assertTrue(
        Double.compare(
            sfcIndexRange.getMinValuesPerDimension()[0],
            range.getMinValuesPerDimension()[0]) == 0);
    Assert.assertTrue(
        Double.compare(
            sfcIndexRange.getMinValuesPerDimension()[1],
            range.getMinValuesPerDimension()[1]) == 0);
    Assert.assertTrue(
        Double.compare(
            sfcIndexRange.getMaxValuesPerDimension()[0],
            range.getMaxValuesPerDimension()[0]) == 0);
    Assert.assertTrue(
        Double.compare(
            sfcIndexRange.getMaxValuesPerDimension()[1],
            range.getMaxValuesPerDimension()[1]) == 0);
  }

  @Test
  public void testHints() {
    final InsertionIds ids = compoundIndexStrategy.getInsertionIds(compoundIndexedRange, 8);

    final List<IndexMetaData> metaData = compoundIndexStrategy.createMetaData();
    for (final IndexMetaData imd : metaData) {
      imd.insertionIdsAdded(ids);
    }

    final byte[][] simpleIndexRanges = simpleIndexStrategy.getQueryPartitionKeys(sfcIndexedRange);
    final QueryRanges sfcIndexRanges = sfcIndexStrategy.getQueryRanges(sfcIndexedRange);
    final List<ByteArrayRange> ranges = new ArrayList<>();
    for (final byte[] r1 : simpleIndexRanges) {
      for (final ByteArrayRange r2 : sfcIndexRanges.getCompositeQueryRanges()) {
        final byte[] start = ByteArrayUtils.combineArrays(r1, r2.getStart());
        final byte[] end = ByteArrayUtils.combineArrays(r1, r2.getEnd());
        ranges.add(new ByteArrayRange(start, end));
      }
    }

    final Set<ByteArrayRange> compoundIndexRangesWithoutHints =
        new HashSet<>(
            compoundIndexStrategy.getQueryRanges(compoundIndexedRange).getCompositeQueryRanges());
    final Set<ByteArrayRange> compoundIndexRangesWithHints =
        new HashSet<>(
            compoundIndexStrategy.getQueryRanges(
                compoundIndexedRange,
                metaData.toArray(new IndexMetaData[metaData.size()])).getCompositeQueryRanges());
    Assert.assertTrue(compoundIndexRangesWithoutHints.containsAll(compoundIndexRangesWithHints));
    Assert.assertTrue(compoundIndexRangesWithHints.containsAll(compoundIndexRangesWithoutHints));

    final List<Persistable> newMetaData =
        PersistenceUtils.fromBinaryAsList(PersistenceUtils.toBinary(metaData));
    final Set<ByteArrayRange> compoundIndexRangesWithHints2 =
        new HashSet<>(
            compoundIndexStrategy.getQueryRanges(
                compoundIndexedRange,
                metaData.toArray(new IndexMetaData[newMetaData.size()])).getCompositeQueryRanges());
    Assert.assertTrue(compoundIndexRangesWithoutHints.containsAll(compoundIndexRangesWithHints2));
    Assert.assertTrue(compoundIndexRangesWithHints2.containsAll(compoundIndexRangesWithoutHints));
  }
}
