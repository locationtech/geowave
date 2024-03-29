/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.bigtable;

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.PropertyStoreImpl;
import org.locationtech.geowave.datastore.bigtable.operations.BigTableOperations;
import org.locationtech.geowave.datastore.bigtable.config.BigTableOptions;
import org.locationtech.geowave.datastore.hbase.HBaseDataStore;
import org.locationtech.geowave.datastore.hbase.config.HBaseOptions;

public class BigTableDataStoreFactory extends BaseDataStoreFactory {
  public BigTableDataStoreFactory(
      final String typeName,
      final String description,
      final StoreFactoryHelper helper) {
    super(typeName, description, helper);
  }

  @Override
  public DataStore createStore(final StoreFactoryOptions options) {
    if (!(options instanceof BigTableOptions)) {
      throw new AssertionError("Expected " + BigTableOptions.class.getSimpleName());
    }

    final BigTableOperations bigtableOperations =
        (BigTableOperations) helper.createOperations(options);

    final HBaseOptions hbaseOptions = ((BigTableOptions) options).getHBaseOptions();
    // make sure to explicitly use the constructor with
    // BigTableDataStatisticsStore
    return new HBaseDataStore(
        new IndexStoreImpl(bigtableOperations, hbaseOptions),
        new AdapterStoreImpl(bigtableOperations, hbaseOptions),
        new DataStatisticsStoreImpl(bigtableOperations, hbaseOptions),
        new AdapterIndexMappingStoreImpl(bigtableOperations, hbaseOptions),
        bigtableOperations,
        hbaseOptions,
        new InternalAdapterStoreImpl(bigtableOperations),
        new PropertyStoreImpl(bigtableOperations, hbaseOptions));
  }
}
