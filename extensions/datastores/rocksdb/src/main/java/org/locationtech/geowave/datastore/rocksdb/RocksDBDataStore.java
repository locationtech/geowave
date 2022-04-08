/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import java.io.Closeable;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.PropertyStoreImpl;
import org.locationtech.geowave.datastore.rocksdb.operations.RocksDBOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

public class RocksDBDataStore extends BaseMapReduceDataStore implements Closeable {
  public RocksDBDataStore(final RocksDBOperations operations, final DataStoreOptions options) {
    super(
        new IndexStoreImpl(operations, options),
        new AdapterStoreImpl(operations, options),
        new DataStatisticsStoreImpl(operations, options),
        new AdapterIndexMappingStoreImpl(operations, options),
        operations,
        options,
        new InternalAdapterStoreImpl(operations),
        new PropertyStoreImpl(operations, options));
  }

  /**
   * This is not a typical resource, it references a static RocksDB resource used by all DataStore
   * instances with common parameters. Closing this is only recommended when the JVM no longer needs
   * any connection to this RocksDB store with common parameters.
   */
  @Override
  public void close() {
    ((RocksDBOperations) baseOperations).close();
  }

  @Override
  public boolean isReverseIterationSupported() {
    return true;
  }
}
