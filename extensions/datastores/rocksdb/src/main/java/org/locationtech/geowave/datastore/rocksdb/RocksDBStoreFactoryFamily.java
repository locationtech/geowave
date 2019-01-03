/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import org.locationtech.geowave.core.store.BaseDataStoreFamily;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.api.DataStore;

public class RocksDBStoreFactoryFamily extends BaseDataStoreFamily {
  private static final String TYPE = "rocksdb";
  private static final String DESCRIPTION = "A GeoWave store backed by data in RocksDB";

  public RocksDBStoreFactoryFamily() {
    super(TYPE, DESCRIPTION, new RocksDBFactoryHelper());
  }

  @Override
  public GenericStoreFactory<DataStore> getDataStoreFactory() {
    return new RocksDBDataStoreFactory(TYPE, DESCRIPTION, new RocksDBFactoryHelper());
  }
}
