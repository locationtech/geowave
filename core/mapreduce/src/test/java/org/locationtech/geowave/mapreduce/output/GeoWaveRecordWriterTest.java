/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.output;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat.GeoWaveRecordWriter;

public class GeoWaveRecordWriterTest {
  private static final String TYPE = "type";

  private DataStore dataStore;
  private GeoWaveRecordWriter writer;

  @Before
  public void setUp() {
    final MemoryRequiredOptions options = new MemoryRequiredOptions();
    options.setGeoWaveNamespace("test_" + getClass().getName() + "_" + System.nanoTime());
    final StoreFactoryFamilySpi store = new MemoryStoreFactoryFamily();
    dataStore = store.getDataStoreFactory().createStore(options);
    writer =
        new GeoWaveRecordWriter(
            dataStore,
            store.getIndexStoreFactory().createStore(options),
            new MemoryAdapterStore());
  }

  @Test
  public void testMissingIndexIsReportedWithoutAddingTheType() throws Exception {
    assertWriteIsRejected("missing");
    assertEquals(0, dataStore.getTypes().length);
    writer.close(null);
  }

  @Test
  public void testKeyNamingAnExistingAndAMissingIndexIsRejected() throws Exception {
    dataStore.addIndex(new NullIndex("existing"));
    assertWriteIsRejected("existing", "missing");
    assertEquals(0, dataStore.getTypes().length);
  }

  @Test
  public void testExistingTypeIsNotWrittenToWhenTheKeyNamesAMissingIndex() throws Exception {
    dataStore.addType(new BinaryDataAdapter(TYPE), new NullIndex("existing"));
    assertWriteIsRejected("missing");
  }

  private void assertWriteIsRejected(final String... indexNames) {
    final IOException e =
        assertThrows(
            IOException.class,
            () -> writer.write(
                new GeoWaveOutputKey<>(new BinaryDataAdapter(TYPE), indexNames),
                Pair.of(new byte[] {1}, new byte[] {2})));
    assertEquals("Cannot write to index '" + Arrays.toString(indexNames) + "'", e.getMessage());
  }
}
