/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.core.store.query.gwql.parse.GWQLParser;

public abstract class AbstractGWQLTest {

  protected DataStore createDataStore() {
    return createDataStore(createDefaultAdapter(), "pop");
  }

  protected DataStore createDataStore(final DataTypeAdapter<?> adapter, final String indexField) {
    final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
    final MemoryRequiredOptions opts = new MemoryRequiredOptions();
    opts.setGeoWaveNamespace("test_" + getClass().getName());
    final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(opts);
    final FieldDescriptor<?> descriptor = adapter.getFieldDescriptor(indexField);
    final Index index =
        AttributeDimensionalityTypeProvider.createIndexForDescriptor(adapter, descriptor, null);
    dataStore.addType(adapter, index);
    return dataStore;
  }

  protected DataTypeAdapter<?> createDefaultAdapter() {
    return BasicDataTypeAdapter.newAdapter("type", DefaultGWQLTestType.class, "pid");
  }

  protected void assertInvalidStatement(
      final DataStore dataStore,
      final String statement,
      final String expectedMessage) {
    try {
      GWQLParser.parseStatement(dataStore, statement);
      fail();
    } catch (GWQLParseException e) {
      // expected
      assertTrue(
          e.getMessage() + " does not contain " + expectedMessage,
          e.getMessage().contains(expectedMessage));
    }
  }

  @GeoWaveDataType
  protected static class DefaultGWQLTestType {
    @GeoWaveField
    private String pid;

    @GeoWaveField
    private Long pop;

    @GeoWaveField
    private String comment;

    public DefaultGWQLTestType() {}

    public DefaultGWQLTestType(final String pid, final Long pop, final String comment) {
      this.pid = pid;
      this.pop = pop;
      this.comment = comment;
    }
  }
}
