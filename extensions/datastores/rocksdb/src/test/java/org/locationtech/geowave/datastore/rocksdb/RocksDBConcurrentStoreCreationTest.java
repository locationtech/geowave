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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.datastore.rocksdb.RocksDBStoreLifetimeTest.POI;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClientCache;

public class RocksDBConcurrentStoreCreationTest {
  private static final String TYPE_NAME = "POI";
  private static final int THREADS = 64;
  private static final int STORES_PER_THREAD = 1000;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @After
  public void closeClients() {
    RocksDBClientCache.getInstance().closeAll();
  }

  /**
   * Spark tasks in local mode create their stores from the job's option map at the same time, as
   * GeoWaveInputFormat.createRecordReader() does.
   */
  @Test
  public void testStoresCreatedConcurrentlyFromOneOptionMapFindTheType() throws Exception {
    final RocksDBOptions options = new RocksDBOptions();
    options.setDirectory(folder.getRoot().getAbsolutePath());
    options.setGeoWaveNamespace("concurrent");
    final DataStore store =
        new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    store.addType(BasicDataTypeAdapter.newAdapter(TYPE_NAME, POI.class, "name"));
    final Map<String, String> optionMap = new DataStorePluginOptions(options).getOptionsAsMap();
    final short adapterId =
        GeoWaveStoreFinder.createInternalAdapterStore(optionMap).getAdapterId(TYPE_NAME);

    final AtomicInteger missing = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        futures.add(pool.submit(() -> {
          start.await();
          for (int i = 0; i < STORES_PER_THREAD; i++) {
            if (GeoWaveStoreFinder.createAdapterStore(optionMap).getAdapter(adapterId) == null) {
              missing.incrementAndGet();
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdown();
    }
    assertEquals(0, missing.get());
  }
}
