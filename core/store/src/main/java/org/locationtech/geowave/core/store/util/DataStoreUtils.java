/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy;
import org.locationtech.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.flatten.FlattenedDataSet;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadDataSingleRow;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.constraints.CustomQueryConstraints.InternalCustomConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions.HintKey;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.PartitionBinningStrategy;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParameterException;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

/*
 */
public class DataStoreUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreUtils.class);
  public static String DEFAULT_GEOWAVE_DIRECTORY =
      System.getProperty("user.home") + File.separator + "geowave";

  public static HintKey<double[]> MAX_RESOLUTION_SUBSAMPLING_PER_DIMENSION =
      new HintKey<>(double[].class);
  public static HintKey<Integer> MAX_RANGE_DECOMPOSITION = new HintKey<>(Integer.class);
  public static HintKey<double[]> TARGET_RESOLUTION_PER_DIMENSION_FOR_HIERARCHICAL_INDEX =
      new HintKey<>(double[].class);
  // we append a 0 byte, 8 bytes of timestamp, and 16 bytes of UUID
  public static final int UNIQUE_ADDED_BYTES = 1 + 8 + 16;
  public static final byte UNIQUE_ID_DELIMITER = 0;

  public static final VisibilityHandler UNCONSTRAINED_VISIBILITY =
      new UnconstrainedVisibilityHandler();

  public static final byte[] EMTPY_VISIBILITY = new byte[] {};

  public static DataTypeAdapter getDataAdapter(
      final DataStorePluginOptions dataStore,
      final String typeName) {
    final Short adapterId = dataStore.createInternalAdapterStore().getAdapterId(typeName);
    if (adapterId == null) {
      return null;
    }

    final DataTypeAdapter adapter = dataStore.createAdapterStore().getAdapter(adapterId);
    if (adapter == null) {
      return null;
    }

    return adapter;
  }

  public static FlattenedUnreadData aggregateFieldData(
      final GeoWaveKey key,
      final GeoWaveValue value,
      final PersistentDataset<Object> commonData,
      final CommonIndexModel model,
      final List<String> commonIndexFieldIds) {
    final byte[] fieldMask = value.getFieldMask();
    final byte[] valueBytes = value.getValue();
    final FlattenedDataSet dataSet =
        DataStoreUtils.decomposeFlattenedFields(
            fieldMask,
            valueBytes,
            value.getVisibility(),
            commonIndexFieldIds.size() - 1);
    final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();

    for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
      final int ordinal = fieldInfo.getFieldPosition();
      if (ordinal < commonIndexFieldIds.size()) {
        final String commonIndexFieldName = commonIndexFieldIds.get(ordinal);
        final FieldReader<?> reader = model.getReader(commonIndexFieldName);
        if (reader != null) {
          final Object fieldValue = reader.readField(fieldInfo.getValue());
          commonData.addValue(commonIndexFieldName, fieldValue);
        } else {
          LOGGER.error("Could not find reader for common index field: " + commonIndexFieldName);
        }
      }
    }
    return dataSet.getFieldsDeferred();
  }

  public static boolean startsWithIfPrefix(
      final byte[] source,
      final byte[] match,
      final boolean prefix) {
    if (!prefix) {
      if (match.length != (source.length)) {
        return false;
      }
    } else if (match.length > (source.length)) {
      return false;
    }
    return ByteArrayUtils.startsWith(source, match);
  }

  public static List<String> getUniqueDimensionFields(final CommonIndexModel model) {
    final List<String> dimensionFieldIds = new ArrayList<>();
    for (final NumericDimensionField<?> dimension : model.getDimensions()) {
      if (!dimensionFieldIds.contains(dimension.getFieldName())) {
        dimensionFieldIds.add(dimension.getFieldName());
      }
    }
    return dimensionFieldIds;
  }

  public static <T> long cardinality(
      final DataStatisticsStore statisticsStore,
      final RowRangeHistogramStatistic rowRangeHistogramStatistic,
      final DataTypeAdapter<?> adapter,
      final Index index,
      final QueryRanges queryRanges) {

    long count = 0;
    for (final SinglePartitionQueryRanges partitionRange : queryRanges.getPartitionQueryRanges()) {
      final RowRangeHistogramValue value =
          statisticsStore.getStatisticValue(
              rowRangeHistogramStatistic,
              CompositeBinningStrategy.getBin(
                  DataTypeBinningStrategy.getBin(adapter),
                  PartitionBinningStrategy.getBin(partitionRange.getPartitionKey())));
      if (value == null) {
        return Long.MAX_VALUE - 1;
      }
      for (final ByteArrayRange range : partitionRange.getSortKeyRanges()) {
        count += value.cardinality(range.getStart(), range.getEnd());
      }
    }
    return count;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <T> InsertionIds getInsertionIdsForEntry(
      final T entry,
      final InternalDataAdapter adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (index instanceof CustomIndexStrategy) {
      return ((CustomIndexStrategy) index).getInsertionIds(entry);
    } else {
      final AdapterPersistenceEncoding encoding = adapter.encode(entry, indexMapping, index);
      return encoding.getInsertionIds(index);
    }
  }

  public static InsertionIds keysToInsertionIds(final GeoWaveKey... geoWaveKeys) {
    final Map<ByteArray, List<byte[]>> sortKeysPerPartition = new HashMap<>();
    for (final GeoWaveKey key : geoWaveKeys) {
      final ByteArray partitionKey = new ByteArray(key.getPartitionKey());
      List<byte[]> sortKeys = sortKeysPerPartition.get(partitionKey);
      if (sortKeys == null) {
        sortKeys = new ArrayList<>();
        sortKeysPerPartition.put(partitionKey, sortKeys);
      }
      sortKeys.add(key.getSortKey());
    }
    final Set<SinglePartitionInsertionIds> insertionIds = new HashSet<>();
    for (final Entry<ByteArray, List<byte[]>> e : sortKeysPerPartition.entrySet()) {
      insertionIds.add(new SinglePartitionInsertionIds(e.getKey().getBytes(), e.getValue()));
    }
    return new InsertionIds(insertionIds);
  }

  public static boolean rowIdsMatch(final GeoWaveKey rowId1, final GeoWaveKey rowId2) {
    if (!Arrays.equals(rowId1.getPartitionKey(), rowId2.getPartitionKey())
        || !Arrays.equals(rowId1.getSortKey(), rowId2.getSortKey())
        || (rowId1.getAdapterId() != rowId2.getAdapterId())) {
      return false;
    }

    if (Arrays.equals(rowId1.getDataId(), rowId2.getDataId())) {
      return true;
    }

    return Arrays.equals(rowId1.getDataId(), rowId2.getDataId());
  }

  public static byte[] removeUniqueId(byte[] dataId) {
    if ((dataId.length < UNIQUE_ADDED_BYTES)
        || (dataId[dataId.length - UNIQUE_ADDED_BYTES] != UNIQUE_ID_DELIMITER)) {
      return dataId;
    }

    dataId = Arrays.copyOfRange(dataId, 0, dataId.length - UNIQUE_ADDED_BYTES);

    return dataId;
  }

  /**
   * Takes a byte array representing a serialized composite group of FieldInfos sharing a common
   * visibility and returns a List of the individual FieldInfos
   *
   * @param bitmask the composite bitmask representing the fields contained within the
   *        flattenedValue
   * @param flattenedValue the serialized composite FieldInfo
   * @param commonVisibility the shared visibility
   * @param maxFieldPosition can short-circuit read and defer decomposition of fields after a given
   *        position
   * @return the dataset that has been read
   */
  public static <T> FlattenedDataSet decomposeFlattenedFields(
      final byte[] bitmask,
      final byte[] flattenedValue,
      final byte[] commonVisibility,
      final int maxFieldPosition) {
    final List<FlattenedFieldInfo> fieldInfoList = new LinkedList<>();
    if ((flattenedValue != null) && (flattenedValue.length > 0)) {
      if ((bitmask != null) && (bitmask.length > 0)) {
        final List<Integer> fieldPositions = BitmaskUtils.getFieldPositions(bitmask);
        final boolean sharedVisibility = fieldPositions.size() > 1;
        if (sharedVisibility) {
          final ByteBuffer input = ByteBuffer.wrap(flattenedValue);
          for (int i = 0; i < fieldPositions.size(); i++) {
            final Integer fieldPosition = fieldPositions.get(i);
            if ((maxFieldPosition > -2) && (fieldPosition > maxFieldPosition)) {
              return new FlattenedDataSet(
                  fieldInfoList,
                  new FlattenedUnreadDataSingleRow(input, i, fieldPositions));
            }
            final int fieldLength = VarintUtils.readUnsignedInt(input);
            final byte[] fieldValueBytes = ByteArrayUtils.safeRead(input, fieldLength);
            fieldInfoList.add(new FlattenedFieldInfo(fieldPosition, fieldValueBytes));
          }
        } else {
          fieldInfoList.add(new FlattenedFieldInfo(fieldPositions.get(0), flattenedValue));
        }
      } else {
        // assume fields are in positional order
        final ByteBuffer input = ByteBuffer.wrap(flattenedValue);
        for (int i = 0; input.hasRemaining(); i++) {
          final Integer fieldPosition = i;
          final int fieldLength = VarintUtils.readUnsignedInt(input);
          final byte[] fieldValueBytes = ByteArrayUtils.safeRead(input, fieldLength);
          fieldInfoList.add(new FlattenedFieldInfo(fieldPosition, fieldValueBytes));
        }
      }
    }
    return new FlattenedDataSet(fieldInfoList, null);
  }

  public static QueryRanges constraintsToQueryRanges(
      final List<MultiDimensionalNumericData> constraints,
      final Index index,
      final double[] targetResolutionPerDimensionForHierarchicalIndex,
      final int maxRanges,
      final IndexMetaData... hints) {
    if ((index instanceof CustomIndex)
        && (constraints != null)
        && (constraints.size() == 1)
        && (constraints.get(0) instanceof InternalCustomConstraints)) {
      return ((CustomIndex) index).getQueryRanges(
          ((InternalCustomConstraints) constraints.get(0)).getCustomConstraints());
    }
    NumericIndexStrategy indexStrategy = index.getIndexStrategy();
    SubStrategy targetIndexStrategy = null;
    if ((targetResolutionPerDimensionForHierarchicalIndex != null)
        && (targetResolutionPerDimensionForHierarchicalIndex.length == indexStrategy.getOrderedDimensionDefinitions().length)) {
      // determine the correct tier to query for the given resolution
      final HierarchicalNumericIndexStrategy strategy =
          CompoundHierarchicalIndexStrategyWrapper.findHierarchicalStrategy(indexStrategy);
      if (strategy != null) {
        final TreeMap<Double, SubStrategy> sortedStrategies = new TreeMap<>();
        for (final SubStrategy subStrategy : strategy.getSubStrategies()) {
          final double[] idRangePerDimension =
              subStrategy.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
          double rangeSum = 0;
          for (final double range : idRangePerDimension) {
            rangeSum += range;
          }
          // sort by the sum of the range in each dimension
          sortedStrategies.put(rangeSum, subStrategy);
        }
        for (final SubStrategy subStrategy : sortedStrategies.descendingMap().values()) {
          final double[] highestPrecisionIdRangePerDimension =
              subStrategy.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
          // if the id range is less than or equal to the target
          // resolution in each dimension, use this substrategy
          boolean withinTargetResolution = true;
          for (int d = 0; d < highestPrecisionIdRangePerDimension.length; d++) {
            if (highestPrecisionIdRangePerDimension[d] > targetResolutionPerDimensionForHierarchicalIndex[d]) {
              withinTargetResolution = false;
              break;
            }
          }
          if (withinTargetResolution) {
            targetIndexStrategy = subStrategy;
            break;
          }
        }
        if (targetIndexStrategy == null) {
          // if there is not a substrategy that is within the target
          // resolution, use the first substrategy (the lowest range
          // per dimension, which is the highest precision)
          targetIndexStrategy = sortedStrategies.firstEntry().getValue();
        }
        indexStrategy = targetIndexStrategy.getIndexStrategy();
      }
    }
    if ((constraints == null) || constraints.isEmpty()) {
      if (targetIndexStrategy != null) {
        // at least use the prefix of a substrategy if chosen
        return new QueryRanges(new byte[][] {targetIndexStrategy.getPrefix()});
      }
      return new QueryRanges(); // implies in negative and
      // positive infinity
    } else {
      final List<QueryRanges> ranges = new ArrayList<>(constraints.size());
      for (final MultiDimensionalNumericData nd : constraints) {
        ranges.add(indexStrategy.getQueryRanges(nd, maxRanges, hints));
      }
      return ranges.size() > 1 ? new QueryRanges(ranges) : ranges.get(0);
    }
  }

  public static String getQualifiedTableName(
      final String tableNamespace,
      final String unqualifiedTableName) {
    return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName
        : tableNamespace + "_" + unqualifiedTableName;
  }

  public static ByteArray ensureUniqueId(final byte[] id, final boolean hasMetadata) {
    final ByteBuffer buf = ByteBuffer.allocate(id.length + UNIQUE_ADDED_BYTES);

    byte[] metadata = null;
    byte[] dataId;
    if (hasMetadata) {
      final int metadataStartIdx = id.length - 12;
      final byte[] lengths = Arrays.copyOfRange(id, metadataStartIdx, id.length);

      final ByteBuffer lengthsBuf = ByteBuffer.wrap(lengths);
      final int adapterIdLength = lengthsBuf.getInt();
      int dataIdLength = lengthsBuf.getInt();
      dataIdLength += UNIQUE_ADDED_BYTES;
      final int duplicates = lengthsBuf.getInt();

      final ByteBuffer newLengths = ByteBuffer.allocate(12);
      newLengths.putInt(adapterIdLength);
      newLengths.putInt(dataIdLength);
      newLengths.putInt(duplicates);
      newLengths.rewind();
      metadata = newLengths.array();
      dataId = Arrays.copyOfRange(id, 0, metadataStartIdx);
    } else {
      dataId = id;
    }

    buf.put(dataId);

    final long timestamp = System.currentTimeMillis();
    buf.put(new byte[] {UNIQUE_ID_DELIMITER});
    final UUID uuid = UUID.randomUUID();
    buf.putLong(timestamp);
    buf.putLong(uuid.getLeastSignificantBits());
    buf.putLong(uuid.getMostSignificantBits());
    if (hasMetadata) {
      buf.put(metadata);
    }

    return new ByteArray(buf.array());
  }

  private static final byte[] OPEN_PAREN_BYTE = "(".getBytes(StringUtils.getGeoWaveCharset());
  private static final byte[] MERGE_VIS_BYTES = ")&(".getBytes(StringUtils.getGeoWaveCharset());
  private static final byte[] CLOSE_PAREN_BYTE = ")".getBytes(StringUtils.getGeoWaveCharset());

  public static byte[] mergeVisibilities(final byte vis1[], final byte vis2[]) {
    if ((vis1 == null) || (vis1.length == 0)) {
      return vis2;
    } else if ((vis2 == null) || (vis2.length == 0)) {
      return vis1;
    } else if (Arrays.equals(vis1, vis2)) {
      return vis1;
    }

    final ByteBuffer buffer =
        ByteBuffer.allocate(
            vis1.length
                + OPEN_PAREN_BYTE.length
                + MERGE_VIS_BYTES.length
                + CLOSE_PAREN_BYTE.length
                + vis2.length);
    buffer.put(OPEN_PAREN_BYTE);
    buffer.put(vis1);
    buffer.put(MERGE_VIS_BYTES);
    buffer.put(vis2);
    buffer.put(CLOSE_PAREN_BYTE);
    return buffer.array();
  }

  public static GeoWaveRow mergeSingleRowValues(
      final GeoWaveRow singleRow,
      final RowTransform rowTransform) {
    if (singleRow.getFieldValues().length < 2) {
      return singleRow;
    }

    // merge all values into a single value
    Mergeable merged = null;

    for (final GeoWaveValue fieldValue : singleRow.getFieldValues()) {
      final Mergeable mergeable =
          rowTransform.getRowAsMergeableObject(
              singleRow.getAdapterId(),
              new ByteArray(fieldValue.getFieldMask()),
              fieldValue.getValue());

      if (merged == null) {
        merged = mergeable;
      } else {
        merged.merge(mergeable);
      }
    }

    final GeoWaveValue[] mergedFieldValues =
        new GeoWaveValue[] {
            new GeoWaveValueImpl(
                singleRow.getFieldValues()[0].getFieldMask(),
                singleRow.getFieldValues()[0].getVisibility(),
                rowTransform.getBinaryFromMergedObject(merged))};

    return new GeoWaveRowImpl(
        new GeoWaveKeyImpl(
            singleRow.getDataId(),
            singleRow.getAdapterId(),
            singleRow.getPartitionKey(),
            singleRow.getSortKey(),
            singleRow.getNumberOfDuplicates()),
        mergedFieldValues);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static boolean mergeData(
      final DataStoreOperations operations,
      final Integer maxRangeDecomposition,
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore) {
    final RowDeleter deleter =
        operations.createRowDeleter(index.getName(), adapterStore, internalAdapterStore);
    try {
      final Map<Short, InternalDataAdapter> mergingAdapters = new HashMap<>();

      final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
      for (final InternalDataAdapter<?> adapter : adapters) {
        if ((adapter.getAdapter() instanceof RowMergingDataAdapter)
            && (((RowMergingDataAdapter) adapter.getAdapter()).getTransform() != null)) {
          mergingAdapters.put(adapter.getAdapterId(), adapter);
        }
      }

      final ReaderParamsBuilder<GeoWaveRow> paramsBuilder =
          new ReaderParamsBuilder<>(
              index,
              adapterStore,
              adapterIndexMappingStore,
              internalAdapterStore,
              GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER).isClientsideRowMerging(
                  true).maxRangeDecomposition(maxRangeDecomposition);

      final short[] adapterIds = new short[1];

      for (final Entry<Short, InternalDataAdapter> adapter : mergingAdapters.entrySet()) {
        adapterIds[0] = adapter.getKey();
        paramsBuilder.adapterIds(adapterIds);

        try (final RowWriter writer = operations.createWriter(index, adapter.getValue());
            final RowReader<GeoWaveRow> reader = operations.createReader(paramsBuilder.build())) {
          final RewritingMergingEntryIterator<?> iterator =
              new RewritingMergingEntryIterator(
                  adapterStore,
                  adapterIndexMappingStore,
                  index,
                  reader,
                  Maps.transformValues(mergingAdapters, v -> v.getAdapter()),
                  writer,
                  deleter);
          while (iterator.hasNext()) {
            iterator.next();
          }
        } catch (final Exception e) {
          LOGGER.error("Exception occurred while merging data.", e);
          throw new RuntimeException(e);
        }
      }
    } finally {
      try {
        deleter.close();
      } catch (final Exception e) {
        LOGGER.warn("Exception occurred when closing deleter.", e);
      }
    }
    return true;
  }

  public static boolean isMergingIteratorRequired(
      final RangeReaderParams<?> readerParams,
      final boolean visibilityEnabled) {
    return readerParams.isClientsideRowMerging()
        || (readerParams.isMixedVisibility() && visibilityEnabled);
  }

  public static List<Index> loadIndices(final IndexStore indexStore, final String indexNames) {
    final List<Index> loadedIndices = Lists.newArrayList();
    // Is there a comma?
    final String[] indices = indexNames.split(",");
    for (final String idxName : indices) {
      final Index index = indexStore.getIndex(idxName);
      if (index == null) {
        throw new ParameterException("Unable to find index with name: " + idxName);
      }
      loadedIndices.add(index);
    }
    return Collections.unmodifiableList(loadedIndices);
  }

  public static List<Index> loadIndices(final DataStore dataStore, final String indexNames) {
    final List<Index> loadedIndices = Lists.newArrayList();
    // Is there a comma?
    final String[] indices = indexNames.split(",");
    final Index[] dataStoreIndices = dataStore.getIndices();
    for (final String idxName : indices) {
      boolean found = false;
      for (final Index index : dataStoreIndices) {
        if (index.getName().equals(idxName)) {
          loadedIndices.add(index);
          found = true;
          break;
        }
      }
      if (!found) {
        throw new ParameterException("Unable to find index with name: " + idxName);
      }
    }
    return Collections.unmodifiableList(loadedIndices);
  }

  public static void safeMetadataDelete(
      final MetadataDeleter deleter,
      final DataStoreOperations operations,
      final MetadataType metadataType,
      final MetadataQuery query) {
    // we need to respect visibilities although this may be much slower
    final MetadataReader reader = operations.createMetadataReader(metadataType);
    try (final CloseableIterator<GeoWaveMetadata> it = reader.query(query)) {
      while (it.hasNext()) {
        final GeoWaveMetadata entry = it.next();
        deleter.delete(
            new MetadataQuery(
                entry.getPrimaryId(),
                entry.getSecondaryId(),
                query.getAuthorizations()));
      }
    }
  }
}
