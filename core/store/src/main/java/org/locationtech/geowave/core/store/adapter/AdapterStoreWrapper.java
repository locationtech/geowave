/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Arrays;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * Given a transient store and a internal adapter store to use to map between internal IDs and
 * external IDs, we can wrap an implementation as a persistent adapter store
 */
public class AdapterStoreWrapper implements PersistentAdapterStore {
  private final TransientAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;

  public AdapterStoreWrapper(
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore) {
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
  }

  @Override
  public void addAdapter(final InternalDataAdapter<?> adapter) {
    adapterStore.addAdapter(adapter.getAdapter());
  }

  @Override
  public InternalDataAdapter<?> getAdapter(final Short adapterId) {
    if (adapterId == null) {
      return null;
    }
    final DataTypeAdapter<?> adapter =
        adapterStore.getAdapter(internalAdapterStore.getTypeName(adapterId));

    if (adapter instanceof InternalDataAdapter) {
      return (InternalDataAdapter<?>) adapter;
    }
    return adapter.asInternalAdapter(adapterId);
  }

  @Override
  public boolean adapterExists(final Short adapterId) {
    if (adapterId != null) {
      return internalAdapterStore.getTypeName(adapterId) != null;
    }
    return false;
  }

  @Override
  public InternalDataAdapter<?>[] getAdapters() {
    final DataTypeAdapter<?>[] adapters = adapterStore.getAdapters();
    return Arrays.stream(adapters).map(adapter -> {
      if (adapter instanceof InternalDataAdapter) {
        return (InternalDataAdapter<?>) adapter;
      }
      final Short adapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
      if (adapterId == null) {
        return null;
      }
      return adapter.asInternalAdapter(adapterId);
    }).toArray(InternalDataAdapter[]::new);
  }

  @Override
  public void removeAll() {
    adapterStore.removeAll();
  }

  @Override
  public void removeAdapter(final Short adapterId) {
    final String typeName = internalAdapterStore.getTypeName(adapterId);
    if (typeName != null) {
      adapterStore.removeAdapter(typeName);
    }
  }
}
