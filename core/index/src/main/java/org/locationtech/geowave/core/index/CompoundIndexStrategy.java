/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import com.google.common.collect.Collections2;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

/**
 * Class that implements a compound index strategy. It combines a PartitionIndexStrategy with a
 * NumericIndexStrategy to enable the addition of a partitioning strategy to any numeric index
 * strategy.
 */
public class CompoundIndexStrategy implements NumericIndexStrategy {

  private PartitionIndexStrategy subStrategy1;
  private NumericIndexStrategy subStrategy2;
  private int defaultMaxDuplication;
  private int metaDataSplit = -1;

  public CompoundIndexStrategy(
      final PartitionIndexStrategy<? extends MultiDimensionalNumericData, ? extends MultiDimensionalNumericData> subStrategy1,
      final NumericIndexStrategy subStrategy2) {
    this.subStrategy1 = subStrategy1;
    this.subStrategy2 = subStrategy2;
    defaultMaxDuplication = (int) Math.ceil(Math.pow(2, getNumberOfDimensions()));
  }

  protected CompoundIndexStrategy() {}

  public PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> getPrimarySubStrategy() {
    return subStrategy1;
  }

  public NumericIndexStrategy getSecondarySubStrategy() {
    return subStrategy2;
  }

  @Override
  public byte[] toBinary() {
    final byte[] delegateBinary1 = PersistenceUtils.toBinary(subStrategy1);
    final byte[] delegateBinary2 = PersistenceUtils.toBinary(subStrategy2);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(delegateBinary1.length)
                + delegateBinary1.length
                + delegateBinary2.length);
    VarintUtils.writeUnsignedInt(delegateBinary1.length, buf);
    buf.put(delegateBinary1);
    buf.put(delegateBinary2);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int delegateBinary1Length = VarintUtils.readUnsignedInt(buf);
    final byte[] delegateBinary1 = ByteArrayUtils.safeRead(buf, delegateBinary1Length);
    final byte[] delegateBinary2 = new byte[buf.remaining()];
    buf.get(delegateBinary2);
    subStrategy1 = (PartitionIndexStrategy) PersistenceUtils.fromBinary(delegateBinary1);
    subStrategy2 = (NumericIndexStrategy) PersistenceUtils.fromBinary(delegateBinary2);

    defaultMaxDuplication = (int) Math.ceil(Math.pow(2, getNumberOfDimensions()));
  }

  /**
   * Get the total number of dimensions from all sub-strategies
   *
   * @return the number of dimensions
   */
  public int getNumberOfDimensions() {
    final NumericDimensionDefinition[] dimensions = subStrategy2.getOrderedDimensionDefinitions();
    if (dimensions == null) {
      return 0;
    }
    return dimensions.length;
  }

  @Override
  public QueryRanges getQueryRanges(
      final MultiDimensionalNumericData indexedRange,
      final IndexMetaData... hints) {
    return getQueryRanges(indexedRange, -1, hints);
  }

  @Override
  public QueryRanges getQueryRanges(
      final MultiDimensionalNumericData indexedRange,
      final int maxEstimatedRangeDecomposition,
      final IndexMetaData... hints) {
    final byte[][] partitionIds =
        subStrategy1.getQueryPartitionKeys(indexedRange, extractHints(hints, 0));
    final QueryRanges queryRanges =
        subStrategy2.getQueryRanges(
            indexedRange,
            maxEstimatedRangeDecomposition,
            extractHints(hints, 1));

    return new QueryRanges(partitionIds, queryRanges);
  }

  @Override
  public InsertionIds getInsertionIds(final MultiDimensionalNumericData indexedData) {
    return getInsertionIds(indexedData, defaultMaxDuplication);
  }

  @Override
  public InsertionIds getInsertionIds(
      final MultiDimensionalNumericData indexedData,
      final int maxEstimatedDuplicateIds) {
    final byte[][] partitionKeys = subStrategy1.getInsertionPartitionKeys(indexedData);
    final InsertionIds insertionIds =
        subStrategy2.getInsertionIds(indexedData, maxEstimatedDuplicateIds);

    final boolean partitionKeysEmpty = (partitionKeys == null) || (partitionKeys.length == 0);
    if ((insertionIds == null)
        || (insertionIds.getPartitionKeys() == null)
        || insertionIds.getPartitionKeys().isEmpty()) {
      if (partitionKeysEmpty) {
        return new InsertionIds();
      } else {
        return new InsertionIds(
            Arrays.stream(partitionKeys).map(
                input -> new SinglePartitionInsertionIds(input)).collect(Collectors.toList()));
      }
    } else if (partitionKeysEmpty) {
      return insertionIds;
    } else {
      final List<SinglePartitionInsertionIds> permutations =
          new ArrayList<>(insertionIds.getPartitionKeys().size() * partitionKeys.length);
      for (final byte[] partitionKey : partitionKeys) {
        permutations.addAll(Collections2.transform(insertionIds.getPartitionKeys(), input -> {
          if (input.getPartitionKey() != null) {
            return new SinglePartitionInsertionIds(
                ByteArrayUtils.combineArrays(partitionKey, input.getPartitionKey()),
                input.getSortKeys());
          } else {
            return new SinglePartitionInsertionIds(partitionKey, input.getSortKeys());
          }
        }));
      }
      return new InsertionIds(permutations);
    }
  }

  @Override
  public MultiDimensionalNumericData getRangeForId(
      final byte[] partitionKey,
      final byte[] sortKey) {
    return subStrategy2.getRangeForId(trimPartitionIdForSortStrategy(partitionKey), sortKey);
  }

  @Override
  public MultiDimensionalCoordinates getCoordinatesPerDimension(
      final byte[] partitionKey,
      final byte[] sortKey) {
    return subStrategy2.getCoordinatesPerDimension(
        trimPartitionIdForSortStrategy(partitionKey),
        sortKey);
  }

  private byte[] trimPartitionIdForSortStrategy(final byte[] partitionKey) {
    final byte[] trimmedKey =
        trimPartitionForSubstrategy(subStrategy1.getPartitionKeyLength(), false, partitionKey);
    return trimmedKey == null ? partitionKey : trimmedKey;
  }

  @Override
  public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
    return subStrategy2.getOrderedDimensionDefinitions();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((subStrategy1 == null) ? 0 : subStrategy1.hashCode());
    result = (prime * result) + ((subStrategy2 == null) ? 0 : subStrategy2.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CompoundIndexStrategy other = (CompoundIndexStrategy) obj;
    if (subStrategy1 == null) {
      if (other.subStrategy1 != null) {
        return false;
      }
    } else if (!subStrategy1.equals(other.subStrategy1)) {
      return false;
    }
    if (subStrategy2 == null) {
      if (other.subStrategy2 != null) {
        return false;
      }
    } else if (!subStrategy2.equals(other.subStrategy2)) {
      return false;
    }
    return true;
  }

  @Override
  public String getId() {
    return StringUtils.intToString(hashCode());
  }

  @Override
  public double[] getHighestPrecisionIdRangePerDimension() {
    return subStrategy2.getHighestPrecisionIdRangePerDimension();
  }

  @Override
  public int getPartitionKeyLength() {
    return subStrategy1.getPartitionKeyLength() + subStrategy2.getPartitionKeyLength();
  }

  @Override
  public List<IndexMetaData> createMetaData() {
    final List<IndexMetaData> result = new ArrayList<>();
    for (final IndexMetaData metaData : (List<IndexMetaData>) subStrategy1.createMetaData()) {
      result.add(
          new CompoundIndexMetaDataWrapper(
              metaData,
              subStrategy1.getPartitionKeyLength(),
              (byte) 0));
    }
    metaDataSplit = result.size();
    for (final IndexMetaData metaData : subStrategy2.createMetaData()) {
      result.add(
          new CompoundIndexMetaDataWrapper(
              metaData,
              subStrategy1.getPartitionKeyLength(),
              (byte) 1));
    }
    return result;
  }

  private int getMetaDataSplit() {
    if (metaDataSplit == -1) {
      metaDataSplit = subStrategy1.createMetaData().size();
    }
    return metaDataSplit;
  }

  private IndexMetaData[] extractHints(final IndexMetaData[] hints, final int indexNo) {
    if ((hints == null) || (hints.length == 0)) {
      return hints;
    }
    final int splitPoint = getMetaDataSplit();
    final int start = (indexNo == 0) ? 0 : splitPoint;
    final int stop = (indexNo == 0) ? splitPoint : hints.length;
    final IndexMetaData[] result = new IndexMetaData[stop - start];
    int p = 0;
    for (int i = start; i < stop; i++) {
      result[p++] = ((CompoundIndexMetaDataWrapper) hints[i]).metaData;
    }
    return result;
  }

  /**
   * Delegate Metadata item for an underlying index. For CompoundIndexStrategy, this delegate wraps
   * the meta data for one of the two indices. The primary function of this class is to extract out
   * the parts of the ByteArrayId that are specific to each index during an 'update' operation.
   */
  protected static class CompoundIndexMetaDataWrapper implements IndexMetaData {

    private IndexMetaData metaData;
    private int partition1Length;
    private byte index;

    protected CompoundIndexMetaDataWrapper() {}

    public CompoundIndexMetaDataWrapper(
        final IndexMetaData metaData,
        final int partition1Length,
        final byte index) {
      super();
      this.partition1Length = partition1Length;
      this.metaData = metaData;
      this.index = index;
    }

    @Override
    public byte[] toBinary() {
      final byte[] metaBytes = PersistenceUtils.toBinary(metaData);
      final int length =
          metaBytes.length
              + VarintUtils.unsignedIntByteLength(metaBytes.length)
              + 1
              + VarintUtils.unsignedIntByteLength(partition1Length);
      final ByteBuffer buf = ByteBuffer.allocate(length);
      VarintUtils.writeUnsignedInt(metaBytes.length, buf);
      buf.put(metaBytes);
      buf.put(index);
      VarintUtils.writeUnsignedInt(partition1Length, buf);
      return buf.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int metaBytesLength = VarintUtils.readUnsignedInt(buf);
      final byte[] metaBytes = new byte[metaBytesLength];
      buf.get(metaBytes);
      metaData = (IndexMetaData) PersistenceUtils.fromBinary(metaBytes);
      index = buf.get();
      partition1Length = VarintUtils.readUnsignedInt(buf);
    }

    @Override
    public void merge(final Mergeable merge) {
      if (merge instanceof CompoundIndexMetaDataWrapper) {
        final CompoundIndexMetaDataWrapper compound = (CompoundIndexMetaDataWrapper) merge;
        metaData.merge(compound.metaData);
      }
    }

    @Override
    public void insertionIdsAdded(final InsertionIds insertionIds) {
      metaData.insertionIdsAdded(trimPartitionForSubstrategy(insertionIds));
    }

    private InsertionIds trimPartitionForSubstrategy(final InsertionIds insertionIds) {
      final List<SinglePartitionInsertionIds> retVal = new ArrayList<>();
      for (final SinglePartitionInsertionIds partitionIds : insertionIds.getPartitionKeys()) {
        final byte[] trimmedPartitionId =
            CompoundIndexStrategy.trimPartitionForSubstrategy(
                partition1Length,
                index == 0,
                partitionIds.getPartitionKey());
        if (trimmedPartitionId == null) {
          return insertionIds;
        } else {
          retVal.add(
              new SinglePartitionInsertionIds(trimmedPartitionId, partitionIds.getSortKeys()));
        }
      }
      return new InsertionIds(retVal);
    }

    @Override
    public void insertionIdsRemoved(final InsertionIds insertionIds) {
      metaData.insertionIdsRemoved(trimPartitionForSubstrategy(insertionIds));
    }

    /** Convert Tiered Index Metadata statistics to a JSON object */
    @Override
    public JSONObject toJSONObject() throws JSONException {
      final JSONObject jo = new JSONObject();
      jo.put("type", "CompoundIndexMetaDataWrapper");
      jo.put("index", index);
      return jo;
    }
  }

  /**
   * @param partition1Length the length of the partition key contributed by the first substrategy
   * @param isFirstSubstrategy if the trimming is for the first substrategy
   * @param compoundPartitionId the compound partition id
   * @return if the partition id requires trimming, the new trimmed key will be returned, otherwise
   *         if trimming isn't necessary it returns null
   */
  private static byte[] trimPartitionForSubstrategy(
      final int partition1Length,
      final boolean isFirstSubstrategy,
      final byte[] compoundPartitionId) {
    if ((partition1Length > 0) && ((compoundPartitionId.length - partition1Length) > 0)) {
      if (isFirstSubstrategy) {
        return Arrays.copyOfRange(compoundPartitionId, 0, partition1Length);
      } else {
        return Arrays.copyOfRange(
            compoundPartitionId,
            partition1Length,
            compoundPartitionId.length);
      }
    }
    return null;
  }

  @Override
  public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
      final MultiDimensionalNumericData dataRange,
      final IndexMetaData... hints) {
    return subStrategy2.getCoordinateRangesPerDimension(dataRange, hints);
  }

  @Override
  public byte[][] getInsertionPartitionKeys(final MultiDimensionalNumericData insertionData) {
    final byte[][] partitionKeys1 = subStrategy1.getInsertionPartitionKeys(insertionData);
    final byte[][] partitionKeys2 = subStrategy2.getInsertionPartitionKeys(insertionData);
    if ((partitionKeys1 == null) || (partitionKeys1.length == 0)) {
      return partitionKeys2;
    }
    if ((partitionKeys2 == null) || (partitionKeys2.length == 0)) {
      return partitionKeys1;
    }
    // return permutations
    final byte[][] partitionKeys = new byte[partitionKeys1.length * partitionKeys2.length][];
    int i = 0;
    for (final byte[] partitionKey1 : partitionKeys1) {
      for (final byte[] partitionKey2 : partitionKeys2) {
        partitionKeys[i++] = ByteArrayUtils.combineArrays(partitionKey1, partitionKey2);
      }
    }
    return partitionKeys;
  }

  @Override
  public byte[][] getQueryPartitionKeys(
      final MultiDimensionalNumericData queryData,
      final IndexMetaData... hints) {
    final byte[][] partitionKeys1 = subStrategy1.getQueryPartitionKeys(queryData, hints);
    final byte[][] partitionKeys2 = subStrategy2.getQueryPartitionKeys(queryData, hints);
    if ((partitionKeys1 == null) || (partitionKeys1.length == 0)) {
      return partitionKeys2;
    }
    if ((partitionKeys2 == null) || (partitionKeys2.length == 0)) {
      return partitionKeys1;
    }
    // return all permutations of partitionKeys
    final byte[][] partitionKeys = new byte[partitionKeys1.length * partitionKeys2.length][];
    int i = 0;
    for (final byte[] partitionKey1 : partitionKeys1) {
      for (final byte[] partitionKey2 : partitionKeys2) {
        partitionKeys[i++] = ByteArrayUtils.combineArrays(partitionKey1, partitionKey2);
      }
    }
    return partitionKeys;
  }

  @Override
  public byte[][] getPredefinedSplits() {
    return subStrategy1.getPredefinedSplits();
  }
}
