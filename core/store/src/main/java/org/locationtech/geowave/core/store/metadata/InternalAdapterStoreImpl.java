/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import java.io.IOException;
import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * This class will persist Adapter Internal Adapter Mappings within an Accumulo table for GeoWave
 * metadata. The mappings will be persisted in an "AIM" column family.
 *
 * <p> There is an LRU cache associated with it so staying in sync with external updates is not
 * practical - it assumes the objects are not updated often or at all. The objects are stored in
 * their own table.
 *
 * <p> Objects are maintained with regard to visibility. The assumption is that a mapping between an
 * adapter and indexing is consistent across all visibility constraints.
 */
public class InternalAdapterStoreImpl implements InternalAdapterStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(InternalAdapterStoreImpl.class);
  private static final Object MUTEX = new Object();
  protected final BiMap<String, Short> cache = Maps.synchronizedBiMap(HashBiMap.create());
  private static final byte[] INTERNAL_TO_EXTERNAL_ID = new byte[] {0};
  private static final byte[] EXTERNAL_TO_INTERNAL_ID = new byte[] {1};

  private static final ByteArray INTERNAL_TO_EXTERNAL_BYTEARRAYID =
      new ByteArray(INTERNAL_TO_EXTERNAL_ID);
  private static final ByteArray EXTERNAL_TO_INTERNAL_BYTEARRAYID =
      new ByteArray(EXTERNAL_TO_INTERNAL_ID);
  private final DataStoreOperations operations;

  public InternalAdapterStoreImpl(final DataStoreOperations operations) {
    this.operations = operations;
  }

  private MetadataReader getReader(final boolean warnIfNotExists) {
    try {
      if (!operations.metadataExists(MetadataType.INTERNAL_ADAPTER)) {
        return null;
      }
    } catch (final IOException e1) {
      if (warnIfNotExists) {
        LOGGER.error("Unable to check for existence of metadata to get object", e1);
      }
      return null;
    }
    return operations.createMetadataReader(MetadataType.INTERNAL_ADAPTER);
  }

  @Override
  public String getTypeName(final short adapterId) {
    return internalGetTypeName(adapterId, true);
  }

  private String internalGetTypeName(final short adapterId, final boolean warnIfNotExists) {
    String typeName = cache.inverse().get(adapterId);
    if (typeName != null) {
      return typeName;
    }
    final MetadataReader reader = getReader(true);
    if (reader == null) {
      if (warnIfNotExists) {
        LOGGER.warn(
            "Adapter ID '"
                + adapterId
                + "' not found. INTERNAL_ADAPTER '"
                + AbstractGeoWavePersistence.METADATA_TABLE
                + "' table does not exist");
      }
      return null;
    }
    try (CloseableIterator<GeoWaveMetadata> it =
        reader.query(
            new MetadataQuery(
                ByteArrayUtils.shortToByteArray(adapterId),
                INTERNAL_TO_EXTERNAL_ID))) {
      if (!it.hasNext()) {
        if (warnIfNotExists) {
          LOGGER.warn("Internal Adapter ID '" + adapterId + "' not found");
        }
        return null;
      }
      typeName = StringUtils.stringFromBinary(it.next().getValue());
      cache.putIfAbsent(typeName, adapterId);
      return typeName;
    }
  }

  @Override
  public Short getAdapterId(final String typeName) {
    return internalGetAdapterId(typeName, true);
  }

  public Short internalGetAdapterId(final String typeName, final boolean warnIfNotExist) {
    final Short id = cache.get(typeName);
    if (id != null) {
      return id;
    }

    final MetadataReader reader = getReader(warnIfNotExist);
    if (reader == null) {
      if (warnIfNotExist) {
        LOGGER.warn(
            "Adapter '"
                + typeName
                + "' not found. INTERNAL_ADAPTER '"
                + AbstractGeoWavePersistence.METADATA_TABLE
                + "' table does not exist");
      }
      return null;
    }
    try (CloseableIterator<GeoWaveMetadata> it =
        reader.query(
            new MetadataQuery(StringUtils.stringToBinary(typeName), EXTERNAL_TO_INTERNAL_ID))) {
      if (!it.hasNext()) {
        if (warnIfNotExist) {
          LOGGER.warn("Adapter '" + typeName + "' not found");
        }
        return null;
      }
      final short adapterId = ByteArrayUtils.byteArrayToShort(it.next().getValue());
      cache.putIfAbsent(typeName, adapterId);
      return adapterId;
    }
  }

  /**
   * This method has a chance of producing a conflicting adapter ID. Whenever possible,
   * {@link #getInitialAdapterId(String)} should be used.
   *
   * @param typeName the type name
   * @return a possibly conflicting adapter ID
   */
  public static short getLazyInitialAdapterId(final String typeName) {
    return (short) (Math.abs((typeName.hashCode() % 127)));
  }

  @Override
  public short getInitialAdapterId(final String typeName) {
    // try to fit it into 1 byte first
    short adapterId = (short) (Math.abs((typeName.hashCode() % 127)));
    for (int i = 0; i < 127; i++) {
      final String adapterIdTypeName = internalGetTypeName(adapterId, false);
      if ((adapterIdTypeName == null) || typeName.equals(adapterIdTypeName)) {
        return adapterId;
      }
      adapterId++;
      if (adapterId > 127) {
        adapterId = 0;
      }
    }
    // try to fit into 2 bytes (only happens if there are more than 127
    // adapters)
    adapterId = (short) (Math.abs((typeName.hashCode() % 16383)));
    for (int i = 0; i < 16256; i++) {
      final String adapterIdTypeName = internalGetTypeName(adapterId, false);
      if ((adapterIdTypeName == null) || typeName.equals(adapterIdTypeName)) {
        return adapterId;
      }
      adapterId++;
      if (adapterId > 16383) {
        adapterId = 128; // it already didn't fit in 1 byte
      }
    }
    // fall back to negative numbers (only happens if there are more than
    // 16,383 adapters)
    final int negativeRange = 0 - Short.MIN_VALUE;
    adapterId = (short) (Math.abs((typeName.hashCode() % negativeRange)) - Short.MIN_VALUE);
    for (int i = 0; i < negativeRange; i++) {
      final String adapterIdTypeName = internalGetTypeName(adapterId, false);
      if ((adapterIdTypeName == null) || typeName.equals(adapterIdTypeName)) {
        return adapterId;
      }
      adapterId++;
      if (adapterId > -1) {
        adapterId = Short.MIN_VALUE;
      }
    }
    return adapterId;
  }

  // ** this introduces a distributed race condition if multiple JVM processes
  // are excuting this method simultaneously
  // care should be taken to either explicitly call this from a single client
  // before running a distributed job, or use a distributed locking mechanism
  // so that internal Adapter Ids are consistent without any race conditions
  @Override
  public short addTypeName(final String typeName) {
    synchronized (MUTEX) {
      Short adapterId = internalGetAdapterId(typeName, false);
      if (adapterId != null) {
        return adapterId;
      }
      adapterId = getInitialAdapterId(typeName);
      try (final MetadataWriter writer =
          operations.createMetadataWriter(MetadataType.INTERNAL_ADAPTER)) {
        if (writer != null) {
          final byte[] adapterIdBytes = ByteArrayUtils.shortToByteArray(adapterId);
          writer.write(
              new GeoWaveMetadata(
                  StringUtils.stringToBinary(typeName),
                  EXTERNAL_TO_INTERNAL_ID,
                  null,
                  adapterIdBytes));
          writer.write(
              new GeoWaveMetadata(
                  adapterIdBytes,
                  INTERNAL_TO_EXTERNAL_ID,
                  null,
                  StringUtils.stringToBinary(typeName)));
        }
      } catch (final Exception e) {
        LOGGER.warn("Unable to close metadata writer", e);
      }
      return adapterId;
    }
  }

  @Override
  public boolean remove(final String typeName) {
    final Short internalAdapterId = getAdapterId(typeName);
    return delete(typeName, internalAdapterId);
  }

  private boolean delete(final String typeName, final Short internalAdapterId) {
    boolean externalDeleted = false;
    if (typeName != null) {
      externalDeleted =
          AbstractGeoWavePersistence.deleteObjects(
              new ByteArray(typeName),
              EXTERNAL_TO_INTERNAL_BYTEARRAYID,
              operations,
              MetadataType.INTERNAL_ADAPTER,
              null);
      cache.remove(typeName);
    }
    boolean internalDeleted = false;
    if (internalAdapterId != null) {
      internalDeleted =
          AbstractGeoWavePersistence.deleteObjects(
              new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId)),
              INTERNAL_TO_EXTERNAL_BYTEARRAYID,
              operations,
              MetadataType.INTERNAL_ADAPTER,
              null);
    }
    return internalDeleted && externalDeleted;
  }

  @Override
  public void removeAll() {
    AbstractGeoWavePersistence.deleteObjects(
        null,
        null,
        operations,
        MetadataType.INTERNAL_ADAPTER,
        null);
    cache.clear();
  }

  @Override
  public boolean remove(final short adapterId) {
    final String typeName = getTypeName(adapterId);
    return delete(typeName, adapterId);
  }

  @Override
  public String[] getTypeNames() {
    final MetadataReader reader = getReader(false);
    if (reader == null) {
      return new String[0];
    }
    final CloseableIterator<GeoWaveMetadata> results =
        reader.query(new MetadataQuery(INTERNAL_TO_EXTERNAL_ID));
    try (CloseableIterator<String> it =
        new CloseableIteratorWrapper<>(
            results,
            Iterators.transform(
                results,
                input -> StringUtils.stringFromBinary(input.getValue())))) {
      return Iterators.toArray(it, String.class);
    }
  }

  @Override
  public short[] getAdapterIds() {
    final MetadataReader reader = getReader(false);
    if (reader == null) {
      return new short[0];
    }
    final CloseableIterator<GeoWaveMetadata> results =
        reader.query(new MetadataQuery(EXTERNAL_TO_INTERNAL_ID));
    try (CloseableIterator<Short> it =
        new CloseableIteratorWrapper<>(
            results,
            Iterators.transform(
                results,
                input -> ByteArrayUtils.byteArrayToShort(input.getValue())))) {
      return ArrayUtils.toPrimitive(Iterators.toArray(it, Short.class));
    }
  }
}
