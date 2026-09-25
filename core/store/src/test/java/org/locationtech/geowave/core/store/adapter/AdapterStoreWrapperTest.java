/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class AdapterStoreWrapperTest {
  private static final String PRESENT_TYPE = "present";
  private static final String MISSING_TYPE = "missing";

  private InternalAdapterStore internalAdapterStore;
  private MemoryAdapterStore transientAdapterStore;
  private AdapterStoreWrapper wrapper;

  @Before
  public void setUp() {
    final MemoryRequiredOptions options = new MemoryRequiredOptions();
    options.setGeoWaveNamespace("test_" + getClass().getName() + "_" + System.nanoTime());
    internalAdapterStore =
        new MemoryStoreFactoryFamily().getInternalAdapterStoreFactory().createStore(options);
    transientAdapterStore = new MemoryAdapterStore() {
      private static final long serialVersionUID = 1L;

      @Override
      public DataTypeAdapter<?> getAdapter(final String typeName) {
        assertNotNull("the transient store was asked for a null type name", typeName);
        return super.getAdapter(typeName);
      }
    };
    transientAdapterStore.addAdapter(new BinaryDataAdapter(PRESENT_TYPE));
    wrapper = new AdapterStoreWrapper(transientAdapterStore, internalAdapterStore);
  }

  @Test
  public void testPresentTypeIsWrappedWithItsAdapterId() {
    final short adapterId = internalAdapterStore.addTypeName(PRESENT_TYPE);
    final InternalDataAdapter<?> adapter = wrapper.getAdapter(adapterId);
    assertNotNull(adapter);
    assertEquals(adapterId, adapter.getAdapterId());
    assertEquals(PRESENT_TYPE, adapter.getTypeName());
  }

  @Test
  public void testTypeMissingFromTransientStoreIsNull() {
    final short adapterId = internalAdapterStore.addTypeName(MISSING_TYPE);
    assertNull(wrapper.getAdapter(adapterId));
  }

  @Test
  public void testAdapterIdWithNoTypeNameIsNull() {
    internalAdapterStore.addTypeName(PRESENT_TYPE);
    assertNull(wrapper.getAdapter((short) 12345));
  }
}
