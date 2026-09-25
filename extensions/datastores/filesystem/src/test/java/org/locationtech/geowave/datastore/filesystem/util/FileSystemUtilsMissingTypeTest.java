/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.AdapterStoreWrapper;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParamsBuilder;

/**
 * A record reader wraps the job's adapters in an {@link AdapterStoreWrapper}, which answers null
 * for a type the job does not have.
 */
public class FileSystemUtilsMissingTypeTest {
  private InternalAdapterStore internalAdapterStore;
  private AdapterStoreWrapper adapterStore;

  @Before
  public void setUp() {
    final MemoryRequiredOptions options = new MemoryRequiredOptions();
    options.setGeoWaveNamespace("test_" + getClass().getName() + "_" + System.nanoTime());
    internalAdapterStore =
        new MemoryStoreFactoryFamily().getInternalAdapterStoreFactory().createStore(options);
    final MemoryAdapterStore jobAdapters = new MemoryAdapterStore();
    jobAdapters.addAdapter(new BinaryDataAdapter("present"));
    adapterStore = new AdapterStoreWrapper(jobAdapters, internalAdapterStore);
  }

  @Test
  public void testPresentType() {
    final short adapterId = internalAdapterStore.addTypeName("present");
    assertEquals(
        Pair.of(false, false),
        FileSystemUtils.isGroupByRowAndIsSortByTime(readerParams(adapterId), adapterId));
  }

  @Test
  public void testMissingTypeIsNamed() {
    final short adapterId = internalAdapterStore.addTypeName("missing");
    final ReaderParams<GeoWaveRow> params = readerParams(adapterId);
    final IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> FileSystemUtils.isGroupByRowAndIsSortByTime(params, adapterId));
    assertTrue(e.getMessage(), e.getMessage().contains("'missing' (adapter ID " + adapterId + ")"));
  }

  private ReaderParams<GeoWaveRow> readerParams(final short adapterId) {
    return new ReaderParamsBuilder<>(
        null,
        adapterStore,
        null,
        internalAdapterStore,
        GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER).adapterIds(adapterId).build();
  }
}
