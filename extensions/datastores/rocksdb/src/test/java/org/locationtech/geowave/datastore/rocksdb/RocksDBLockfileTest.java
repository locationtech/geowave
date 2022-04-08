/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.CompoundIndexStrategy;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.index.IndexPluginOptions.PartitionStrategy;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.rocksdb.RocksDBException;
import com.google.common.collect.Iterators;

public class RocksDBLockfileTest {
  private static final String DEFAULT_DB_DIRECTORY = "./target/rocksdb";
  private static final String POI_TYPE_NAME = "POI";

  @Test
  public void testIndex() throws RocksDBException {
    testLockfile(1, false);
  }

  @Test
  public void testSecondaryIndex() throws RocksDBException {
    testLockfile(1, true);
  }


  @Test
  public void testIndexMultithreaded() throws RocksDBException {
    testLockfile(8, false);
  }

  @Test
  public void testSecondaryIndexMultithreaded() throws RocksDBException {
    testLockfile(8, true);
  }


  private void testLockfile(final int numThreads, final boolean secondaryIndexing) {
    final RocksDBOptions options = new RocksDBOptions();
    options.setDirectory(DEFAULT_DB_DIRECTORY);
    options.getStoreOptions().setSecondaryIndexing(secondaryIndexing);
    final DataStore store =
        new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    store.deleteAll();
    store.addType(BasicDataTypeAdapter.newAdapter(POI_TYPE_NAME, POI.class, "name"));
    Index index =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            store,
            new AttributeIndexOptions(POI_TYPE_NAME, "latitude"));
    index =
        new CustomNameIndex(
            new CompoundIndexStrategy(new RoundRobinKeyIndexStrategy(32), index.getIndexStrategy()),
            index.getIndexModel(),
            index.getName() + "_" + PartitionStrategy.ROUND_ROBIN.name() + "_" + 32);
    final Index latAttributeIndex = new IndexWrapper(index);
    store.addIndex(POI_TYPE_NAME, latAttributeIndex);
    final DataStore store2 =
        new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    IntStream.range(0, numThreads).mapToObj(i -> CompletableFuture.runAsync(() -> {
      double offset = i * numThreads;
      try (Writer<POI> w = store.createWriter(POI_TYPE_NAME)) {
        w.write(new POI("name" + offset, offset, offset));
      }
      try (
          CloseableIterator<POI> poiIt = store2.query(QueryBuilder.newBuilder(POI.class).build())) {
        if (numThreads == 1) {
          Assert.assertEquals(1, Iterators.size(poiIt));
        } else {
          Assert.assertTrue(Iterators.size(poiIt) >= 1);
        }
      }
      offset++;
      try (Writer<POI> w = store2.createWriter(POI_TYPE_NAME)) {
        w.write(new POI("name" + offset, offset, offset));
      }
      try (CloseableIterator<POI> poiIt = store.query(QueryBuilder.newBuilder(POI.class).build())) {
        if (numThreads == 1) {
          Assert.assertEquals(2, Iterators.size(poiIt));
        } else {
          Assert.assertTrue(Iterators.size(poiIt) >= 2);
        }
      }
      offset++;
      try (Writer<POI> w = store2.createWriter(POI_TYPE_NAME)) {
        w.write(new POI("name" + offset, offset, offset));
        try (CloseableIterator<POI> poiIt =
            store.query(QueryBuilder.newBuilder(POI.class).build())) {
          if (numThreads == 1) {
            Assert.assertEquals(2, Iterators.size(poiIt));
          } else {
            Assert.assertTrue(Iterators.size(poiIt) >= 2);
          }
        }
        w.flush();
        try (CloseableIterator<POI> poiIt =
            store.query(QueryBuilder.newBuilder(POI.class).build())) {
          if (numThreads == 1) {
            Assert.assertEquals(3, Iterators.size(poiIt));
          } else {
            Assert.assertTrue(Iterators.size(poiIt) >= 3);
          }
        }
      }
      try (
          CloseableIterator<POI> poiIt = store2.query(QueryBuilder.newBuilder(POI.class).build())) {
        if (numThreads == 1) {
          Assert.assertEquals(3, Iterators.size(poiIt));
        } else {
          Assert.assertTrue(Iterators.size(poiIt) >= 3);
        }
      }
    }));
    store.deleteAll();
  }

  public static class IndexWrapper implements Index {
    private Index index;

    public IndexWrapper() {}

    public IndexWrapper(final Index index) {
      this.index = index;
    }

    @Override
    public byte[] toBinary() {
      return PersistenceUtils.toBinary(index);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      index = (Index) PersistenceUtils.fromBinary(bytes);
    }

    @Override
    public String getName() {
      return index.getName();
    }

    @Override
    public NumericIndexStrategy getIndexStrategy() {
      return index.getIndexStrategy();
    }

    @Override
    public CommonIndexModel getIndexModel() {
      return index.getIndexModel();
    }
  }

  @GeoWaveDataType
  private static class POI {
    @GeoWaveField
    private final String name;
    @GeoWaveField
    private final Double latitude;
    @GeoWaveField
    private final Double longitude;

    protected POI() {
      this.name = null;
      this.latitude = null;
      this.longitude = null;
    }

    public POI(final String name, final Double latitude, final Double longitude) {
      this.name = name;
      this.latitude = latitude;
      this.longitude = longitude;
    }
  }

}
