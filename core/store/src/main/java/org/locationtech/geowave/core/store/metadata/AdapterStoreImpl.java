/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will persist Data Adapters within an Accumulo table for GeoWave metadata. The adapters
 * will be persisted in an "ADAPTER" column family.
 *
 * <p> There is an LRU cache associated with it so staying in sync with external updates is not
 * practical - it assumes the objects are not updated often or at all. The objects are stored in
 * their own table.
 */
public class AdapterStoreImpl extends AbstractGeoWavePersistence<InternalDataAdapter<?>> implements
    PersistentAdapterStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdapterStoreImpl.class);

  public AdapterStoreImpl(final DataStoreOperations operations, final DataStoreOptions options) {
    super(operations, options, MetadataType.ADAPTER);
  }

  @Override
  public void addAdapter(final InternalDataAdapter<?> adapter) {
    addObject(adapter);
  }

  @Override
  public InternalDataAdapter<?> getAdapter(final Short internalAdapterId) {
    if (internalAdapterId == null) {
      LOGGER.warn("Cannot get adapter for null internal ID");
      return null;
    }
    return getObject(new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId)), null);
  }

  @Override
  protected InternalDataAdapter<?> fromValue(final GeoWaveMetadata entry) {
    final DataTypeAdapter<?> adapter =
        (DataTypeAdapter<?>) PersistenceUtils.fromBinary(entry.getValue());
    if (adapter instanceof InternalDataAdapter) {
      return (InternalDataAdapter<?>) adapter;
    }
    return adapter.asInternalAdapter(ByteArrayUtils.byteArrayToShort(entry.getPrimaryId()));
  }

  @Override
  protected byte[] getValue(final InternalDataAdapter<?> object) {
    return PersistenceUtils.toBinary(object);
  }

  @Override
  public boolean adapterExists(final Short internalAdapterId) {
    if (internalAdapterId == null) {
      LOGGER.warn("Cannot check existence of adapter for null internal ID");
      return false;
    }
    return objectExists(new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId)), null);
  }

  @Override
  protected ByteArray getPrimaryId(final InternalDataAdapter<?> persistedObject) {
    return new ByteArray(ByteArrayUtils.shortToByteArray(persistedObject.getAdapterId()));
  }

  @Override
  public InternalDataAdapter<?>[] getAdapters() {
    // use a map with the adapter ID as key to ensure only one adapter per unique ID
    final Map<Short, InternalDataAdapter<?>> adapters = new HashMap<>();
    try (CloseableIterator<InternalDataAdapter<?>> iter = getObjects()) {
      while (iter.hasNext()) {
        final InternalDataAdapter<?> adapter = iter.next();
        adapters.put(adapter.getAdapterId(), adapter);
      }
    }
    return adapters.values().toArray(new InternalDataAdapter[adapters.size()]);
  }

  @Override
  public void removeAdapter(final Short internalAdapterId) {
    if (internalAdapterId == null) {
      LOGGER.warn("Cannot remove adapter for null internal ID");
      return;
    }
    remove(new ByteArray(ByteArrayUtils.shortToByteArray(internalAdapterId)));
  }
}
