/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.numeric;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

/**
 * The Binned Numeric Dataset class creates an object that associates a multi-dimensional index
 * range to a particular bin ID.
 */
public class BinnedNumericDataset implements MultiDimensionalNumericData {
  private byte[] binId;
  private MultiDimensionalNumericData indexRanges;

  public BinnedNumericDataset() {}

  /**
   * @param binId a unique ID associated with the BinnedQuery object
   * @param indexRanges multi-dimensional range data
   */
  public BinnedNumericDataset(final byte[] binId, final MultiDimensionalNumericData indexRanges) {
    this.binId = binId;
    this.indexRanges = indexRanges;
  }

  /** @return an array of NumericData objects associated with this object. */
  @Override
  public NumericData[] getDataPerDimension() {
    return indexRanges.getDataPerDimension();
  }

  /** @return an array of max values associated with each dimension */
  @Override
  public Double[] getMaxValuesPerDimension() {
    return indexRanges.getMaxValuesPerDimension();
  }

  /** @return an array of min values associated with each dimension */
  @Override
  public Double[] getMinValuesPerDimension() {
    return indexRanges.getMinValuesPerDimension();
  }

  /** @return an array of centroid values associated with each dimension */
  @Override
  public Double[] getCentroidPerDimension() {
    return indexRanges.getCentroidPerDimension();
  }

  /** @return the number of total dimensions */
  @Override
  public int getDimensionCount() {
    return indexRanges.getDimensionCount();
  }

  /** @return a unique ID associated with this object */
  public byte[] getBinId() {
    return binId;
  }

  /**
   * This method is responsible for translating a query into appropriate normalized and binned (if
   * necessary) queries that can be used by the underlying index implementation. For example, for
   * unbounded dimensions such as time, an incoming query of July 2012 to July 2013 may get
   * translated into 2 binned queries representing the 2012 portion of the query and the 2013
   * portion, each normalized to millis from the beginning of the year.
   *
   * @param numericData the incoming query into the index implementation, to be translated into
   *        normalized, binned queries
   * @param dimensionDefinitions the definition for the dimensions
   * @return normalized indexes
   */
  public static List<BinnedNumericDataset> applyBins(
      final MultiDimensionalNumericData numericData,
      final NumericDimensionDefinition[] dimensionDefinitions) {
    if (dimensionDefinitions.length == 0) {
      return Collections.emptyList();
    }

    final BinRange[][] binRangesPerDimension =
        getBinnedRangesPerDimension(numericData, dimensionDefinitions);
    // now we need to combine all permutations of bin ranges into
    // BinnedQuery objects
    final List<BinnedNumericDataset> binnedQueries = new ArrayList<>();
    generatePermutations(binRangesPerDimension, binnedQueries, 0, null);
    return binnedQueries;
  }

  private static void generatePermutations(
      final BinRange[][] binRangesPerDimension,
      final List<BinnedNumericDataset> result,
      final int dimension,
      final BinnedNumericDataset current) {
    if (dimension == binRangesPerDimension.length) {
      result.add(current);
      return;
    }

    for (int i = 0; i < binRangesPerDimension[dimension].length; ++i) {
      BinnedNumericDataset next;
      final NumericData[] rangePerDimension;
      if (current == null) {
        rangePerDimension = new NumericRange[binRangesPerDimension.length];
        next =
            new BinnedNumericDataset(
                binRangesPerDimension[dimension][i].getBinId(),
                new BasicNumericDataset(rangePerDimension));

      } else {
        // because binned queries were intended to be immutable,
        // re-instantiate the object
        rangePerDimension = new NumericRange[binRangesPerDimension.length];
        for (int d = 0; d < dimension; d++) {
          rangePerDimension[d] = current.getDataPerDimension()[d];
        }
        final byte[] combinedBinId =
            ByteArrayUtils.combineArrays(
                current.getBinId(),
                binRangesPerDimension[dimension][i].getBinId());
        next = new BinnedNumericDataset(combinedBinId, new BasicNumericDataset(rangePerDimension));
      }

      rangePerDimension[dimension] =
          new NumericRange(
              binRangesPerDimension[dimension][i].getNormalizedMin(),
              binRangesPerDimension[dimension][i].getNormalizedMax());
      generatePermutations(binRangesPerDimension, result, dimension + 1, next);
    }
  }

  public static BinRange[][] getBinnedRangesPerDimension(
      final MultiDimensionalNumericData numericData,
      final NumericDimensionDefinition[] dimensionDefinitions) {
    if (dimensionDefinitions.length == 0) {
      return new BinRange[0][];
    }
    final BinRange[][] binRangesPerDimension = new BinRange[dimensionDefinitions.length][];
    for (int d = 0; d < dimensionDefinitions.length; d++) {
      binRangesPerDimension[d] =
          dimensionDefinitions[d].getNormalizedRanges(numericData.getDataPerDimension()[d]);
    }
    return binRangesPerDimension;
  }

  @Override
  public boolean isEmpty() {
    return indexRanges.isEmpty();
  }

  @Override
  public byte[] toBinary() {
    final byte[] indexRangesBinary = PersistenceUtils.toBinary(indexRanges);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(binId.length)
                + indexRangesBinary.length
                + binId.length);
    VarintUtils.writeUnsignedInt(binId.length, buf);
    buf.put(binId);
    buf.put(indexRangesBinary);
    return null;
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    binId = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));

    final byte[] indexRangesBinary = new byte[buf.remaining()];
    buf.get(indexRangesBinary);
    indexRanges = (MultiDimensionalNumericData) PersistenceUtils.fromBinary(indexRangesBinary);
  }
}
