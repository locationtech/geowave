/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
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
    store.addType(new POIBasicDataAdapter(POI_TYPE_NAME));
    Index index =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            store,
            new AttributeIndexOptions(POI_TYPE_NAME, POIBasicDataAdapter.LATITUDE_FIELD_NAME));
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
      try (CloseableIterator<POI> poiIt = store2.query((Query) QueryBuilder.newBuilder().build())) {
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
      try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
        if (numThreads == 1) {
          Assert.assertEquals(2, Iterators.size(poiIt));
        } else {
          Assert.assertTrue(Iterators.size(poiIt) >= 2);
        }
      }
      offset++;
      try (Writer<POI> w = store2.createWriter(POI_TYPE_NAME)) {
        w.write(new POI("name" + offset, offset, offset));
        try (
            CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
          if (numThreads == 1) {
            Assert.assertEquals(2, Iterators.size(poiIt));
          } else {
            Assert.assertTrue(Iterators.size(poiIt) >= 2);
          }
        }
        w.flush();
        try (
            CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
          if (numThreads == 1) {
            Assert.assertEquals(3, Iterators.size(poiIt));
          } else {
            Assert.assertTrue(Iterators.size(poiIt) >= 3);
          }
        }
      }
      try (CloseableIterator<POI> poiIt = store2.query((Query) QueryBuilder.newBuilder().build())) {
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
  private static class POI {
    private final String name;
    private final Double latitude;
    private final Double longitude;

    public POI(final String name, final Double latitude, final Double longitude) {
      this.name = name;
      this.latitude = latitude;
      this.longitude = longitude;
    }
  }

  /**
   * The simplest way to implement a data adapter for a custom data type is to extend the
   * {@link BasicDataTypeAdapter} and implement the methods that read and write the custom type.
   */
  public static class POIBasicDataAdapter extends BasicDataTypeAdapter<POI> {
    public static final String NAME_FIELD_NAME = "name";
    public static final String LATITUDE_FIELD_NAME = "lat";
    public static final String LONGITUDE_FIELD_NAME = "lon";

    private static final FieldDescriptor<String> NAME_FIELD =
        new FieldDescriptorBuilder<>(String.class).fieldName(NAME_FIELD_NAME).build();
    private static final FieldDescriptor<Double> LATITUDE_FIELD =
        new FieldDescriptorBuilder<>(Double.class).fieldName(LATITUDE_FIELD_NAME).build();
    private static final FieldDescriptor<Double> LONGITUDE_FIELD =
        new FieldDescriptorBuilder<>(Double.class).fieldName(LONGITUDE_FIELD_NAME).build();
    private static final FieldDescriptor<?>[] FIELDS =
        new FieldDescriptor[] {NAME_FIELD, LATITUDE_FIELD, LONGITUDE_FIELD};

    public POIBasicDataAdapter() {}

    public POIBasicDataAdapter(final String typeName) {
      super(typeName, FIELDS, NAME_FIELD_NAME);
    }

    @Override
    public Object getFieldValue(final POI entry, final String fieldName) {
      switch (fieldName) {
        case NAME_FIELD_NAME:
          return entry.name;
        case LATITUDE_FIELD_NAME:
          return entry.latitude;
        case LONGITUDE_FIELD_NAME:
          return entry.longitude;
      }
      return null;
    }

    @Override
    public POI buildObject(final Object[] fieldValues) {
      return new POI((String) fieldValues[0], (Double) fieldValues[1], (Double) fieldValues[2]);
    }

  }

}
