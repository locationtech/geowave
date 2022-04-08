/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.splits;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterStoreWrapper;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic.IndexMetaDataSetValue;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic.PartitionsValue;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplitsProvider.class);

  private static final BigInteger TWO = BigInteger.valueOf(2);

  public SplitsProvider() {}

  /** Read the metadata table to get tablets and match up ranges to them. */
  public List<InputSplit> getSplits(
      final DataStoreOperations operations,
      final CommonQueryOptions commonOptions,
      final DataTypeQueryOptions<?> typeOptions,
      final IndexQueryOptions indexOptions,
      final QueryConstraints constraints,
      final TransientAdapterStore adapterStore,
      final DataStatisticsStore statsStore,
      final InternalAdapterStore internalAdapterStore,
      final IndexStore indexStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final JobContext context,
      final Integer minSplits,
      final Integer maxSplits) throws IOException, InterruptedException {

    final Map<Pair<Index, ByteArray>, RowRangeHistogramValue> statsCache = new HashMap<>();

    final List<InputSplit> retVal = new ArrayList<>();
    final TreeSet<IntermediateSplitInfo> splits = new TreeSet<>();
    final Map<String, List<Short>> indexIdToAdaptersMap = new HashMap<>();

    for (final Pair<Index, List<Short>> indexAdapterIdPair : BaseDataStoreUtils.getAdaptersWithMinimalSetOfIndices(
        typeOptions.getTypeNames(),
        indexOptions.getIndexName(),
        adapterStore,
        internalAdapterStore,
        adapterIndexMappingStore,
        indexStore,
        constraints)) {
      QueryConstraints indexAdapterConstraints;
      if (constraints instanceof AdapterAndIndexBasedQueryConstraints) {
        final List<Short> adapters = indexAdapterIdPair.getRight();
        DataTypeAdapter<?> adapter = null;
        // in practice this is used for CQL and you can't have multiple
        // types/adapters
        if (adapters.size() == 1) {
          final String typeName = internalAdapterStore.getTypeName(adapters.get(0));
          if (typeName != null) {
            adapter = adapterStore.getAdapter(typeName);
          }
        }
        if (adapter == null) {
          indexAdapterConstraints = constraints;
          LOGGER.info("Unable to find type matching an adapter dependent query");
        } else {
          indexAdapterConstraints =
              ((AdapterAndIndexBasedQueryConstraints) constraints).createQueryConstraints(
                  adapter.asInternalAdapter(adapters.get(0)),
                  indexAdapterIdPair.getLeft(),
                  adapterIndexMappingStore.getMapping(
                      adapters.get(0),
                      indexAdapterIdPair.getLeft().getName()));
          if (indexAdapterConstraints == null) {
            continue;
          }
          // make sure we pass along the new constraints to the record
          // reader - for spark on YARN (not localy though), job
          // configuration is immutable so while picking up the
          // appropriate constraint from the configuration is more
          // efficient, also do a check for
          // AdapterAndIndexBasedQueryConstraints within the Record Reader
          // itself
          GeoWaveInputFormat.setQueryConstraints(
              context.getConfiguration(),
              indexAdapterConstraints);
        }
      } else {
        indexAdapterConstraints = constraints;
      }

      indexIdToAdaptersMap.put(
          indexAdapterIdPair.getKey().getName(),
          indexAdapterIdPair.getValue());
      IndexMetaData[] indexMetadata = null;
      if (indexAdapterConstraints != null) {
        final IndexMetaDataSetValue statValue =
            InternalStatisticsHelper.getIndexMetadata(
                indexAdapterIdPair.getLeft(),
                indexAdapterIdPair.getRight(),
                new AdapterStoreWrapper(adapterStore, internalAdapterStore),
                statsStore,
                commonOptions.getAuthorizations());
        if (statValue != null) {
          indexMetadata = statValue.toArray();
        }
      }
      populateIntermediateSplits(
          splits,
          operations,
          indexAdapterIdPair.getLeft(),
          indexAdapterIdPair.getValue(),
          statsCache,
          adapterStore,
          internalAdapterStore,
          statsStore,
          maxSplits,
          indexAdapterConstraints,
          (double[]) commonOptions.getHints().get(
              DataStoreUtils.TARGET_RESOLUTION_PER_DIMENSION_FOR_HIERARCHICAL_INDEX),
          indexMetadata,
          commonOptions.getAuthorizations());
    }

    // this is an incremental algorithm, it may be better use the target
    // split count to drive it (ie. to get 3 splits this will split 1
    // large
    // range into two down the middle and then split one of those ranges
    // down the middle to get 3, rather than splitting one range into
    // thirds)
    final List<IntermediateSplitInfo> unsplittable = new ArrayList<>();
    if (!statsCache.isEmpty()
        && !splits.isEmpty()
        && (minSplits != null)
        && (splits.size() < minSplits)) {
      // set the ranges to at least min splits
      do {
        // remove the highest range, split it into 2 and add both
        // back,
        // increasing the size by 1
        final IntermediateSplitInfo highestSplit = splits.pollLast();
        final IntermediateSplitInfo otherSplit = highestSplit.split(statsCache);
        // When we can't split the highest split we remove it and
        // attempt the second highest
        // working our way up the split set.
        if (otherSplit == null) {
          unsplittable.add(highestSplit);
        } else {
          splits.add(highestSplit);
          splits.add(otherSplit);
        }
      } while ((splits.size() != 0) && ((splits.size() + unsplittable.size()) < minSplits));

      // Add all unsplittable splits back to splits array
      splits.addAll(unsplittable);

      if (splits.size() < minSplits) {
        LOGGER.warn("Truly unable to meet split count. Actual Count: " + splits.size());
      }
    } else if (((maxSplits != null) && (maxSplits > 0)) && (splits.size() > maxSplits)) {
      // merge splits to fit within max splits
      do {
        // this is the naive approach, remove the lowest two ranges
        // and merge them, decreasing the size by 1

        // TODO Ideally merge takes into account locations (as well
        // as possibly the index as a secondary criteria) to limit
        // the number of locations/indices
        final IntermediateSplitInfo lowestSplit = splits.pollFirst();
        final IntermediateSplitInfo nextLowestSplit = splits.pollFirst();
        lowestSplit.merge(nextLowestSplit);
        splits.add(lowestSplit);
      } while (splits.size() > maxSplits);
    }

    for (final IntermediateSplitInfo split : splits) {
      retVal.add(
          split.toFinalSplit(
              statsStore,
              adapterStore,
              internalAdapterStore,
              indexIdToAdaptersMap,
              commonOptions.getAuthorizations()));
    }
    return retVal;
  }

  protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
      final TreeSet<IntermediateSplitInfo> splits,
      final DataStoreOperations operations,
      final Index index,
      final List<Short> adapterIds,
      final Map<Pair<Index, ByteArray>, RowRangeHistogramValue> statsCache,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final DataStatisticsStore statsStore,
      final Integer maxSplits,
      final QueryConstraints constraints,
      final double[] targetResolutionPerDimensionForHierarchicalIndex,
      final IndexMetaData[] indexMetadata,
      final String[] authorizations) throws IOException {

    // Build list of row ranges from query
    List<ByteArrayRange> ranges = null;
    if (constraints != null) {
      final List<MultiDimensionalNumericData> indexConstraints =
          constraints.getIndexConstraints(index);
      if ((maxSplits != null) && (maxSplits > 0)) {
        ranges =
            DataStoreUtils.constraintsToQueryRanges(
                indexConstraints,
                index,
                targetResolutionPerDimensionForHierarchicalIndex,
                maxSplits,
                indexMetadata).getCompositeQueryRanges();
      } else {
        ranges =
            DataStoreUtils.constraintsToQueryRanges(
                indexConstraints,
                index,
                targetResolutionPerDimensionForHierarchicalIndex,
                -1,
                indexMetadata).getCompositeQueryRanges();
      }
    }
    final List<RangeLocationPair> rangeList = new ArrayList<>();
    final PersistentAdapterStore persistentAdapterStore =
        new AdapterStoreWrapper(adapterStore, internalAdapterStore);
    if (ranges == null) {
      final PartitionsValue statistics =
          InternalStatisticsHelper.getPartitions(
              index,
              adapterIds,
              persistentAdapterStore,
              statsStore,
              authorizations);

      // Try to get ranges from histogram statistics
      if (statistics != null) {
        final Set<ByteArray> partitionKeys = statistics.getValue();
        for (final ByteArray partitionKey : partitionKeys) {
          final GeoWaveRowRange gwRange =
              new GeoWaveRowRange(partitionKey.getBytes(), null, null, true, true);
          final double cardinality =
              getCardinality(
                  getHistStats(
                      index,
                      adapterIds,
                      persistentAdapterStore,
                      statsStore,
                      statsCache,
                      partitionKey,
                      authorizations),
                  gwRange);
          rangeList.add(
              new RangeLocationPair(
                  gwRange,
                  cardinality <= 0 ? 0 : cardinality < 1 ? 1.0 : cardinality));
        }
      } else {
        // add one all-inclusive range
        rangeList.add(
            new RangeLocationPair(new GeoWaveRowRange(null, null, null, true, false), 0.0));
      }
    } else {
      for (final ByteArrayRange range : ranges) {
        final GeoWaveRowRange gwRange =
            SplitsProvider.toRowRange(range, index.getIndexStrategy().getPartitionKeyLength());

        final double cardinality =
            getCardinality(
                getHistStats(
                    index,
                    adapterIds,
                    persistentAdapterStore,
                    statsStore,
                    statsCache,
                    new ByteArray(gwRange.getPartitionKey()),
                    authorizations),
                gwRange);

        rangeList.add(
            new RangeLocationPair(
                gwRange,
                cardinality <= 0 ? 0 : cardinality < 1 ? 1.0 : cardinality));
      }
    }

    final Map<String, SplitInfo> splitInfo = new HashMap<>();

    if (!rangeList.isEmpty()) {
      splitInfo.put(index.getName(), new SplitInfo(index, rangeList));
      splits.add(new IntermediateSplitInfo(splitInfo, this));
    }

    return splits;
  }

  protected double getCardinality(
      final RowRangeHistogramValue rangeStats,
      final GeoWaveRowRange range) {
    if (range == null) {
      if (rangeStats != null) {
        return rangeStats.getTotalCount();
      } else {
        // with an infinite range and no histogram we have no info to
        // base a cardinality on
        return 0;
      }
    }

    return rangeStats == null ? 0.0
        : rangeStats.cardinality(range.getStartSortKey(), range.getEndSortKey());
  }

  protected RowRangeHistogramValue getHistStats(
      final Index index,
      final List<Short> adapterIds,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statsStore,
      final Map<Pair<Index, ByteArray>, RowRangeHistogramValue> statsCache,
      final ByteArray partitionKey,
      final String[] authorizations) throws IOException {
    final Pair<Index, ByteArray> key = Pair.of(index, partitionKey);
    RowRangeHistogramValue rangeStats = statsCache.get(key);

    if (rangeStats == null) {
      try {
        rangeStats =
            InternalStatisticsHelper.getRangeStats(
                index,
                adapterIds,
                adapterStore,
                statsStore,
                partitionKey,
                authorizations);
        if (rangeStats != null) {
          statsCache.put(key, rangeStats);
        }
      } catch (final Exception e) {
        throw new IOException(e);
      }
    }
    return rangeStats;
  }

  protected static byte[] getKeyFromBigInteger(final BigInteger value, final int numBytes) {
    // TODO: does this account for the two extra bytes on BigInteger?
    final byte[] valueBytes = value.toByteArray();
    final byte[] bytes = new byte[numBytes];
    final int pos = Math.abs(numBytes - valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes, pos, Math.min(valueBytes.length, bytes.length));
    return bytes;
  }

  protected static BigInteger getRange(final GeoWaveRowRange range, final int cardinality) {
    return getEnd(range, cardinality).subtract(getStart(range, cardinality));
  }

  protected static BigInteger getStart(final GeoWaveRowRange range, final int cardinality) {
    final byte[] start = range.getStartSortKey();
    byte[] startBytes;
    if (!range.isInfiniteStartSortKey() && (start != null)) {
      startBytes = extractBytes(start, cardinality);
    } else {
      startBytes = extractBytes(new byte[] {}, cardinality);
    }
    return new BigInteger(startBytes);
  }

  protected static BigInteger getEnd(final GeoWaveRowRange range, final int cardinality) {
    final byte[] end = range.getEndSortKey();
    byte[] endBytes;
    if (!range.isInfiniteStopSortKey() && (end != null)) {
      endBytes = extractBytes(end, cardinality);
    } else {
      endBytes = extractBytes(new byte[] {}, cardinality, true);
    }

    return new BigInteger(endBytes);
  }

  protected static double getRangeLength(final GeoWaveRowRange range) {
    if ((range == null) || (range.getStartSortKey() == null) || (range.getEndSortKey() == null)) {
      return 1;
    }
    final byte[] start = range.getStartSortKey();
    final byte[] end = range.getEndSortKey();

    final int maxDepth = Math.max(end.length, start.length);
    final BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
    final BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
    return endBI.subtract(startBI).doubleValue();
  }

  protected static byte[] getMidpoint(final GeoWaveRowRange range) {
    if ((range.getStartSortKey() == null) || (range.getEndSortKey() == null)) {
      return null;
    }

    final byte[] start = range.getStartSortKey();
    final byte[] end = range.getEndSortKey();
    if (Arrays.equals(start, end)) {
      return null;
    }
    final int maxDepth = Math.max(end.length, start.length);
    final BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
    final BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
    final BigInteger rangeBI = endBI.subtract(startBI);
    if (rangeBI.equals(BigInteger.ZERO) || rangeBI.equals(BigInteger.ONE)) {
      return end;
    }
    final byte[] valueBytes = rangeBI.divide(TWO).add(startBI).toByteArray();
    final byte[] bytes = new byte[valueBytes.length - 2];
    System.arraycopy(valueBytes, 2, bytes, 0, bytes.length);
    return bytes;
  }

  public static byte[] extractBytes(final byte[] seq, final int numBytes) {
    return extractBytes(seq, numBytes, false);
  }

  protected static byte[] extractBytes(
      final byte[] seq,
      final int numBytes,
      final boolean infiniteEndKey) {
    final byte[] bytes = new byte[numBytes + 2];
    bytes[0] = 1;
    bytes[1] = 0;
    for (int i = 0; i < numBytes; i++) {
      if (i >= seq.length) {
        if (infiniteEndKey) {
          // -1 is 0xff
          bytes[i + 2] = -1;
        } else {
          bytes[i + 2] = 0;
        }
      } else {
        bytes[i + 2] = seq[i];
      }
    }
    return bytes;
  }

  public static GeoWaveRowRange toRowRange(
      final ByteArrayRange range,
      final int partitionKeyLength) {
    final byte[] startRow = range.getStart() == null ? null : range.getStart();
    final byte[] stopRow = range.getEnd() == null ? null : range.getEnd();

    if (partitionKeyLength <= 0) {
      return new GeoWaveRowRange(null, startRow, stopRow, true, false);
    } else {
      byte[] partitionKey;
      boolean partitionKeyDiffers = false;
      if ((startRow == null) && (stopRow == null)) {
        return new GeoWaveRowRange(null, null, null, true, true);
      } else if (startRow != null) {
        partitionKey = ArrayUtils.subarray(startRow, 0, partitionKeyLength);
        if (stopRow != null) {
          partitionKeyDiffers =
              !Arrays.equals(partitionKey, ArrayUtils.subarray(stopRow, 0, partitionKeyLength));
        }
      } else {
        partitionKey = ArrayUtils.subarray(stopRow, 0, partitionKeyLength);
      }
      return new GeoWaveRowRange(
          partitionKey,
          startRow == null ? null
              : (partitionKeyLength == startRow.length ? null
                  : ArrayUtils.subarray(startRow, partitionKeyLength, startRow.length)),
          partitionKeyDiffers ? null
              : (stopRow == null ? null
                  : (partitionKeyLength == stopRow.length ? null
                      : ArrayUtils.subarray(stopRow, partitionKeyLength, stopRow.length))),
          true,
          partitionKeyDiffers);
    }
  }

  public static ByteArrayRange fromRowRange(final GeoWaveRowRange range) {

    if ((range.getPartitionKey() == null) || (range.getPartitionKey().length == 0)) {
      final byte[] startKey = (range.getStartSortKey() == null) ? null : range.getStartSortKey();
      final byte[] endKey = (range.getEndSortKey() == null) ? null : range.getEndSortKey();

      return new ByteArrayRange(startKey, endKey);
    } else {
      final byte[] startKey =
          (range.getStartSortKey() == null) ? range.getPartitionKey()
              : ArrayUtils.addAll(range.getPartitionKey(), range.getStartSortKey());

      final byte[] endKey =
          (range.getEndSortKey() == null) ? ByteArrayUtils.getNextPrefix(range.getPartitionKey())
              : ArrayUtils.addAll(range.getPartitionKey(), range.getEndSortKey());

      return new ByteArrayRange(startKey, endKey);
    }
  }

  public static byte[] getInclusiveEndKey(final byte[] endKey) {
    final byte[] inclusiveEndKey = new byte[endKey.length + 1];

    System.arraycopy(endKey, 0, inclusiveEndKey, 0, inclusiveEndKey.length - 1);

    return inclusiveEndKey;
  }
}
