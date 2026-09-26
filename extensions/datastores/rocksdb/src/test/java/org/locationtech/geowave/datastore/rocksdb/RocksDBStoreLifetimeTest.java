/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClientCache;
import com.google.common.collect.Iterators;

public class RocksDBStoreLifetimeTest {
  private static final String TYPE_NAME = "POI";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @After
  public void closeClients() {
    RocksDBClientCache.getInstance().closeAll();
  }

  @Test
  public void testWritingContinuesAfterAnotherStoreOnTheDirectoryIsClosed() throws IOException {
    final RocksDBOptions options = options();
    final DataStore store = createStore(options);
    addType(store);
    final DataStore other = createStore(options);
    try (Writer<POI> writer = other.createWriter(TYPE_NAME)) {
      write(writer, "a", 5000);
      writer.flush();
      // what GeoWaveGTDataStore.dispose() does
      ((Closeable) store).close();
      write(writer, "b", 5000);
    }
    assertEquals(10000, count(other));
    assertEquals(10000, count(createStore(options)));
  }

  @Test
  public void testQueriesAndWritesContinueWhileOtherStoresOnTheDirectoryAreClosed()
      throws Exception {
    final RocksDBOptions options = options();
    final DataStore store = createStore(options);
    addType(store);
    try (Writer<POI> writer = store.createWriter(TYPE_NAME)) {
      write(writer, "a", 2000);
    }
    final AtomicBoolean done = new AtomicBoolean();
    final ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      final List<Future<?>> others = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        // as concurrent GeoServer requests do, through GeoServerRestClient and GeoWaveGTDataStore
        others.add(pool.submit(() -> {
          while (!done.get()) {
            final DataStore other = createStore(options);
            assertEquals(1, other.getTypes().length);
            ((Closeable) other).close();
          }
          return null;
        }));
      }
      int expected = 2000;
      for (int i = 0; i < 20; i++) {
        assertEquals(expected, count(store));
        try (Writer<POI> writer = store.createWriter(TYPE_NAME)) {
          write(writer, "b" + i + "_", 100);
        }
        expected += 100;
      }
      done.set(true);
      for (final Future<?> other : others) {
        other.get();
      }
    } finally {
      done.set(true);
      pool.shutdown();
    }
    assertEquals(4000, count(store));
  }

  @Test
  public void testStoreFailsOnceClosed() throws IOException {
    final DataStore store = createStore(options());
    addType(store);
    final DataStore other = createStore(options());
    ((Closeable) store).close();
    assertThrows(IllegalStateException.class, () -> count(store));
    assertEquals(0, count(other));
  }

  @Test
  public void testQueryLeftOpenAcrossDeleteAllFails() {
    final DataStore store = createStore(options());
    addType(store);
    try (Writer<POI> writer = store.createWriter(TYPE_NAME)) {
      write(writer, "a", 2000);
    }
    final CloseableIterator<POI> results = store.query(QueryBuilder.newBuilder(POI.class).build());
    for (int i = 0; i < 10; i++) {
      results.next();
    }
    store.deleteAll();
    assertThrows(IllegalStateException.class, results::hasNext);
    results.close();

    addType(store);
    try (Writer<POI> writer = store.createWriter(TYPE_NAME)) {
      write(writer, "b", 100);
    }
    assertEquals(100, count(store));
  }

  private RocksDBOptions options() {
    final RocksDBOptions options = new RocksDBOptions();
    options.setDirectory(folder.getRoot().getAbsolutePath());
    return options;
  }

  private static DataStore createStore(final RocksDBOptions options) {
    return new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
  }

  private static void addType(final DataStore store) {
    store.addType(BasicDataTypeAdapter.newAdapter(TYPE_NAME, POI.class, "name"));
    store.addIndex(
        TYPE_NAME,
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            store,
            new AttributeIndexOptions(TYPE_NAME, "latitude")));
  }

  private static void write(final Writer<POI> writer, final String prefix, final int count) {
    for (int i = 0; i < count; i++) {
      writer.write(new POI(prefix + i, (i % 180) - 90.0, (i % 360) - 180.0));
    }
  }

  private static int count(final DataStore store) {
    try (CloseableIterator<POI> it = store.query(QueryBuilder.newBuilder(POI.class).build())) {
      return Iterators.size(it);
    }
  }

  @GeoWaveDataType
  public static class POI {
    @GeoWaveField
    private final String name;
    @GeoWaveField
    private final Double latitude;
    @GeoWaveField
    private final Double longitude;

    protected POI() {
      name = null;
      latitude = null;
      longitude = null;
    }

    public POI(final String name, final Double latitude, final Double longitude) {
      this.name = name;
      this.latitude = latitude;
      this.longitude = longitude;
    }
  }
}
