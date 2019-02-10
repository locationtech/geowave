/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu;

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.kudu.config.KuduOptions;
import org.locationtech.geowave.datastore.kudu.operations.KuduOperations;

public class KuduDataStoreFactory extends BaseDataStoreFactory {

  public KuduDataStoreFactory(
      final String typeName,
      final String description,
      final StoreFactoryHelper helper) {
    super(typeName, description, helper);
  }

  @Override
  public DataStore createStore(final StoreFactoryOptions options) {
    if (!(options instanceof KuduOptions)) {
      throw new AssertionError("Expected " + KuduOptions.class.getSimpleName());
    }

    return new KuduDataStore(
        (KuduOperations) helper.createOperations(options),
        options.getStoreOptions());
  }
}
