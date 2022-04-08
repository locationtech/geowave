/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.merge;

import java.awt.image.SampleModel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.locationtech.geowave.adapter.raster.adapter.RasterTile;
import org.locationtech.geowave.adapter.raster.util.SampleModelPersistenceUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MultiAdapterServerMergeStrategy<T extends Persistable> implements
    ServerMergeStrategy,
    Mergeable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MultiAdapterServerMergeStrategy.class);
  // the purpose for these maps instead of a list of samplemodel and adapter
  // ID pairs is to allow for multiple adapters to share the same sample model
  protected Map<Integer, SampleModel> sampleModels = new HashMap<>();
  public Map<Short, Integer> adapterIdToSampleModelKey = new HashMap<>();

  public Map<Integer, RasterTileMergeStrategy<T>> childMergeStrategies = new HashMap<>();
  public Map<Short, Integer> adapterIdToChildMergeStrategyKey = new HashMap<>();

  public MultiAdapterServerMergeStrategy() {}

  public MultiAdapterServerMergeStrategy(
      final SingleAdapterServerMergeStrategy singleAdapterMergeStrategy) {
    sampleModels.put(0, singleAdapterMergeStrategy.sampleModel);
    adapterIdToSampleModelKey.put(singleAdapterMergeStrategy.internalAdapterId, 0);
    childMergeStrategies.put(0, singleAdapterMergeStrategy.mergeStrategy);
    adapterIdToChildMergeStrategyKey.put(singleAdapterMergeStrategy.internalAdapterId, 0);
  }

  public SampleModel getSampleModel(final short internalAdapterId) {
    synchronized (this) {
      final Integer sampleModelId = adapterIdToSampleModelKey.get(internalAdapterId);
      if (sampleModelId != null) {
        return sampleModels.get(sampleModelId);
      }
      return null;
    }
  }

  public RasterTileMergeStrategy<T> getChildMergeStrategy(final short internalAdapterId) {
    synchronized (this) {
      final Integer childMergeStrategyId = adapterIdToChildMergeStrategyKey.get(internalAdapterId);
      if (childMergeStrategyId != null) {
        return childMergeStrategies.get(childMergeStrategyId);
      }
      return null;
    }
  }

  @Override
  public void merge(final Mergeable merge) {
    synchronized (this) {
      if ((merge != null) && (merge instanceof MultiAdapterServerMergeStrategy)) {
        final MultiAdapterServerMergeStrategy<T> other = (MultiAdapterServerMergeStrategy) merge;
        mergeMaps(
            sampleModels,
            adapterIdToSampleModelKey,
            other.sampleModels,
            other.adapterIdToSampleModelKey);
        mergeMaps(
            childMergeStrategies,
            adapterIdToChildMergeStrategyKey,
            other.childMergeStrategies,
            other.adapterIdToChildMergeStrategyKey);
      }
    }
  }

  private static <T> void mergeMaps(
      final Map<Integer, T> thisValues,
      final Map<Short, Integer> thisAdapterIdToValueKeys,
      final Map<Integer, T> otherValues,
      final Map<Short, Integer> otherAdapterIdToValueKeys) {
    // this was generalized to apply to both sample models and merge
    // strategies, comments refer to sample models but in general it is also
    // applied to merge strategies

    // first check for sample models that exist in 'other' that do
    // not exist in 'this'
    for (final Entry<Integer, T> sampleModelEntry : otherValues.entrySet()) {
      if (!thisValues.containsValue(sampleModelEntry.getValue())) {
        // we need to add this sample model
        final List<Short> adapterIds = new ArrayList<>();
        // find all adapter IDs associated with this sample
        // model
        for (final Entry<Short, Integer> adapterIdEntry : otherAdapterIdToValueKeys.entrySet()) {
          if (adapterIdEntry.getValue().equals(sampleModelEntry.getKey())) {
            adapterIds.add(adapterIdEntry.getKey());
          }
        }
        if (!adapterIds.isEmpty()) {
          addValue(adapterIds, sampleModelEntry.getValue(), thisValues, thisAdapterIdToValueKeys);
        }
      }
    }
    // next check for adapter IDs that exist in 'other' that do not
    // exist in 'this'
    for (final Entry<Short, Integer> adapterIdEntry : otherAdapterIdToValueKeys.entrySet()) {
      if (!thisAdapterIdToValueKeys.containsKey(adapterIdEntry.getKey())) {
        // find the sample model associated with the adapter ID
        // in 'other' and find what Integer it is with in 'this'
        final T sampleModel = otherValues.get(adapterIdEntry.getValue());
        if (sampleModel != null) {
          // because the previous step added any missing
          // sample models, it should be a fair assumption
          // that the sample model exists in 'this'
          for (final Entry<Integer, T> sampleModelEntry : thisValues.entrySet()) {
            if (sampleModel.equals(sampleModelEntry.getValue())) {
              // add the sample model key to the
              // adapterIdToSampleModelKey map
              thisAdapterIdToValueKeys.put(adapterIdEntry.getKey(), sampleModelEntry.getKey());
              break;
            }
          }
        }
      }
    }
  }

  private static synchronized <T> void addValue(
      final List<Short> adapterIds,
      final T sampleModel,
      final Map<Integer, T> values,
      final Map<Short, Integer> adapterIdToValueKeys) {
    int nextId = 1;
    boolean idAvailable = false;
    while (!idAvailable) {
      boolean idMatched = false;
      for (final Integer id : values.keySet()) {
        if (nextId == id.intValue()) {
          idMatched = true;
          break;
        }
      }
      if (idMatched) {
        // try the next incremental ID
        nextId++;
      } else {
        // its not matched so we can use it
        idAvailable = true;
      }
    }
    values.put(nextId, sampleModel);
    for (final Short adapterId : adapterIds) {
      adapterIdToValueKeys.put(adapterId, nextId);
    }
  }

  @SuppressFBWarnings(
      value = {"DLS_DEAD_LOCAL_STORE"},
      justification = "Incorrect warning, sampleModelBinary used")
  @Override
  public byte[] toBinary() {
    int byteCount = 0;
    final List<byte[]> sampleModelBinaries = new ArrayList<>();
    final List<Integer> sampleModelKeys = new ArrayList<>();
    int successfullySerializedModels = 0;
    int successfullySerializedModelAdapters = 0;
    final Set<Integer> successfullySerializedModelIds = new HashSet<>();
    for (final Entry<Integer, SampleModel> entry : sampleModels.entrySet()) {
      final SampleModel sampleModel = entry.getValue();
      try {
        final byte[] sampleModelBinary =
            SampleModelPersistenceUtils.getSampleModelBinary(sampleModel);
        byteCount += sampleModelBinary.length;
        byteCount += VarintUtils.unsignedIntByteLength(sampleModelBinary.length);
        byteCount += VarintUtils.unsignedIntByteLength(entry.getKey());
        sampleModelBinaries.add(sampleModelBinary);
        sampleModelKeys.add(entry.getKey());
        successfullySerializedModels++;
        successfullySerializedModelIds.add(entry.getKey());
      } catch (final Exception e) {
        LOGGER.warn("Unable to serialize sample model", e);
      }
    }
    byteCount += VarintUtils.unsignedIntByteLength(successfullySerializedModelIds.size());

    for (final Entry<Short, Integer> entry : adapterIdToSampleModelKey.entrySet()) {
      if (successfullySerializedModelIds.contains(entry.getValue())) {
        byteCount += VarintUtils.unsignedShortByteLength(entry.getKey());
        byteCount += VarintUtils.unsignedIntByteLength(entry.getValue());
        successfullySerializedModelAdapters++;
      }
    }
    byteCount += VarintUtils.unsignedIntByteLength(successfullySerializedModelAdapters);

    final List<byte[]> mergeStrategyBinaries = new ArrayList<>();
    final List<Integer> mergeStrategyKeys = new ArrayList<>();
    int successfullySerializedMergeStrategies = 0;
    int successfullySerializedMergeAdapters = 0;
    final Set<Integer> successfullySerializedMergeIds = new HashSet<>();
    for (final Entry<Integer, RasterTileMergeStrategy<T>> entry : childMergeStrategies.entrySet()) {
      final RasterTileMergeStrategy<T> mergeStrategy = entry.getValue();
      final byte[] mergeStrategyBinary = PersistenceUtils.toBinary(mergeStrategy);
      byteCount += mergeStrategyBinary.length;
      byteCount += VarintUtils.unsignedIntByteLength(mergeStrategyBinary.length);
      byteCount += VarintUtils.unsignedIntByteLength(entry.getKey());
      mergeStrategyBinaries.add(mergeStrategyBinary);
      mergeStrategyKeys.add(entry.getKey());
      successfullySerializedMergeStrategies++;
      successfullySerializedMergeIds.add(entry.getKey());
    }
    byteCount += VarintUtils.unsignedIntByteLength(successfullySerializedMergeStrategies);

    for (final Entry<Short, Integer> entry : adapterIdToChildMergeStrategyKey.entrySet()) {
      if (successfullySerializedMergeIds.contains(entry.getValue())) {
        byteCount += VarintUtils.unsignedShortByteLength(entry.getKey());
        byteCount += VarintUtils.unsignedIntByteLength(entry.getValue());
        successfullySerializedMergeAdapters++;
      }
    }
    byteCount += VarintUtils.unsignedIntByteLength(successfullySerializedMergeAdapters);

    final ByteBuffer buf = ByteBuffer.allocate(byteCount);
    VarintUtils.writeUnsignedInt(successfullySerializedModels, buf);
    for (int i = 0; i < successfullySerializedModels; i++) {
      final byte[] sampleModelBinary = sampleModelBinaries.get(i);
      VarintUtils.writeUnsignedInt(sampleModelBinary.length, buf);
      buf.put(sampleModelBinary);
      VarintUtils.writeUnsignedInt(sampleModelKeys.get(i), buf);
    }

    VarintUtils.writeUnsignedInt(successfullySerializedModelAdapters, buf);
    for (final Entry<Short, Integer> entry : adapterIdToSampleModelKey.entrySet()) {
      if (successfullySerializedModelIds.contains(entry.getValue())) {
        VarintUtils.writeUnsignedShort(entry.getKey(), buf);
        VarintUtils.writeUnsignedInt(entry.getValue(), buf);
      }
    }
    VarintUtils.writeUnsignedInt(successfullySerializedMergeStrategies, buf);
    for (int i = 0; i < successfullySerializedMergeStrategies; i++) {
      final byte[] mergeStrategyBinary = mergeStrategyBinaries.get(i);
      VarintUtils.writeUnsignedInt(mergeStrategyBinary.length, buf);
      buf.put(mergeStrategyBinary);
      VarintUtils.writeUnsignedInt(mergeStrategyKeys.get(i), buf);
    }

    VarintUtils.writeUnsignedInt(successfullySerializedMergeAdapters, buf);
    for (final Entry<Short, Integer> entry : adapterIdToChildMergeStrategyKey.entrySet()) {
      if (successfullySerializedModelIds.contains(entry.getValue())) {
        VarintUtils.writeUnsignedShort(entry.getKey(), buf);
        VarintUtils.writeUnsignedInt(entry.getValue(), buf);
      }
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int sampleModelSize = VarintUtils.readUnsignedInt(buf);
    sampleModels = new HashMap<>(sampleModelSize);
    for (int i = 0; i < sampleModelSize; i++) {
      final byte[] sampleModelBinary =
          ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      if (sampleModelBinary.length > 0) {
        try {
          final int sampleModelKey = VarintUtils.readUnsignedInt(buf);
          final SampleModel sampleModel =
              SampleModelPersistenceUtils.getSampleModel(sampleModelBinary);
          sampleModels.put(sampleModelKey, sampleModel);
        } catch (final Exception e) {
          LOGGER.warn("Unable to deserialize sample model", e);
        }
      } else {
        LOGGER.warn("Sample model binary is empty, unable to deserialize");
      }
    }
    final int sampleModelAdapterIdSize = VarintUtils.readUnsignedInt(buf);
    adapterIdToSampleModelKey = new HashMap<>(sampleModelAdapterIdSize);
    for (int i = 0; i < sampleModelAdapterIdSize; i++) {
      adapterIdToSampleModelKey.put(
          VarintUtils.readUnsignedShort(buf),
          VarintUtils.readUnsignedInt(buf));
    }

    final int mergeStrategySize = VarintUtils.readUnsignedInt(buf);
    childMergeStrategies = new HashMap<>(mergeStrategySize);
    for (int i = 0; i < mergeStrategySize; i++) {
      final byte[] mergeStrategyBinary =
          ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
      if (mergeStrategyBinary.length > 0) {
        try {
          final RasterTileMergeStrategy mergeStrategy =
              (RasterTileMergeStrategy) PersistenceUtils.fromBinary(mergeStrategyBinary);
          final int mergeStrategyKey = VarintUtils.readUnsignedInt(buf);
          if (mergeStrategy != null) {
            childMergeStrategies.put(mergeStrategyKey, mergeStrategy);
          }
        } catch (final Exception e) {
          LOGGER.warn("Unable to deserialize merge strategy", e);
        }
      } else {
        LOGGER.warn("Merge strategy binary is empty, unable to deserialize");
      }
    }
    final int mergeStrategyAdapterIdSize = VarintUtils.readUnsignedInt(buf);
    adapterIdToChildMergeStrategyKey = new HashMap<>(mergeStrategyAdapterIdSize);
    for (int i = 0; i < mergeStrategyAdapterIdSize; i++) {
      adapterIdToChildMergeStrategyKey.put(
          VarintUtils.readUnsignedShort(buf),
          VarintUtils.readUnsignedInt(buf));
    }
  }

  // public T getMetadata(
  // final GridCoverage tileGridCoverage,
  // final Map originalCoverageProperties,
  // final RasterDataAdapter dataAdapter ) {
  // final RasterTileMergeStrategy<T> childMergeStrategy =
  // getChildMergeStrategy(dataAdapter.getAdapterId());
  // if (childMergeStrategy != null) {
  // return childMergeStrategy.getMetadata(
  // tileGridCoverage,
  // dataAdapter);
  // }
  // return null;
  // }

  @Override
  public void merge(
      final RasterTile thisTile,
      final RasterTile nextTile,
      final short internalAdapterId) {
    final RasterTileMergeStrategy<T> childMergeStrategy = getChildMergeStrategy(internalAdapterId);

    if (childMergeStrategy != null) {
      childMergeStrategy.merge(thisTile, nextTile, getSampleModel(internalAdapterId));
    }
  }
}
