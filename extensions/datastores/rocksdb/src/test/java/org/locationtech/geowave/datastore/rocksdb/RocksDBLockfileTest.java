/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import org.junit.Assert;
import org.junit.Test;
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
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.rocksdb.RocksDBException;
import com.google.common.collect.Iterators;

public class RocksDBLockfileTest {
  private static final String DEFAULT_DB_DIRECTORY = "./target/rocksdb";
  private static final String POI_TYPE_NAME = "POI";

  @Test
  public void testSecondaryIndex() throws RocksDBException {
    final RocksDBOptions options = new RocksDBOptions();
    options.setDirectory(DEFAULT_DB_DIRECTORY);
    options.getStoreOptions().setSecondaryIndexing(true);
    final DataStore store =
        new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    store.deleteAll();
    store.addType(new POIBasicDataAdapter(POI_TYPE_NAME));
    final Index latAttributeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            store,
            new AttributeIndexOptions(POI_TYPE_NAME, POIBasicDataAdapter.LATITUDE_FIELD_NAME));
    store.addIndex(POI_TYPE_NAME, latAttributeIndex);
    try (Writer<POI> w = store.createWriter(POI_TYPE_NAME)) {
      w.write(new POI("name1", 0.0, 0.0));
    }
    try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
      Assert.assertEquals(1, Iterators.size(poiIt));
    }
    try (Writer<POI> w = store.createWriter(POI_TYPE_NAME)) {
      w.write(new POI("name2", 1.0, 1.0));
    }
    try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
      Assert.assertEquals(2, Iterators.size(poiIt));
    }
    try (Writer<POI> w = store.createWriter(POI_TYPE_NAME)) {
      w.write(new POI("name3", 2.0, 2.0));
      try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
        Assert.assertEquals(2, Iterators.size(poiIt));
      }
      w.flush();
      try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
        Assert.assertEquals(3, Iterators.size(poiIt));
      }
    }
    try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
      Assert.assertEquals(3, Iterators.size(poiIt));
    }
    store.deleteAll();
  }

  @Test
  public void testMultiDatastoreAttributeIndex() throws RocksDBException {
    final RocksDBOptions options = new RocksDBOptions();
    options.setDirectory(DEFAULT_DB_DIRECTORY);
    final DataStore store =
        new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    store.deleteAll();
    store.addType(new POIBasicDataAdapter(POI_TYPE_NAME));
    final Index latAttributeIndex =
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            store,
            new AttributeIndexOptions(POI_TYPE_NAME, POIBasicDataAdapter.LATITUDE_FIELD_NAME));
    store.addIndex(POI_TYPE_NAME, latAttributeIndex);
    final DataStore store2 =
        new RocksDBStoreFactoryFamily().getDataStoreFactory().createStore(options);
    try (Writer<POI> w = store.createWriter(POI_TYPE_NAME)) {
      w.write(new POI("name1", 0.0, 0.0));
    }
    try (CloseableIterator<POI> poiIt = store2.query((Query) QueryBuilder.newBuilder().build())) {
      Assert.assertEquals(1, Iterators.size(poiIt));
    }
    try (Writer<POI> w = store2.createWriter(POI_TYPE_NAME)) {
      w.write(new POI("name2", 1.0, 1.0));
    }
    try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
      Assert.assertEquals(2, Iterators.size(poiIt));
    }
    try (Writer<POI> w = store2.createWriter(POI_TYPE_NAME)) {
      w.write(new POI("name3", 2.0, 2.0));
      try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
        Assert.assertEquals(2, Iterators.size(poiIt));
      }
      w.flush();
      try (CloseableIterator<POI> poiIt = store.query((Query) QueryBuilder.newBuilder().build())) {
        Assert.assertEquals(3, Iterators.size(poiIt));
      }
    }
    try (CloseableIterator<POI> poiIt = store2.query((Query) QueryBuilder.newBuilder().build())) {
      Assert.assertEquals(3, Iterators.size(poiIt));
    }
    store.deleteAll();
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
