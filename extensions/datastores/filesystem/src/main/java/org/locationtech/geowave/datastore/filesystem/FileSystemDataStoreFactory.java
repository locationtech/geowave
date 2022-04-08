/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem;

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.filesystem.config.FileSystemOptions;
import org.locationtech.geowave.datastore.filesystem.operations.FileSystemOperations;

public class FileSystemDataStoreFactory extends BaseDataStoreFactory {

  public FileSystemDataStoreFactory(
      final String typeName,
      final String description,
      final StoreFactoryHelper helper) {
    super(typeName, description, helper);
  }

  @Override
  public DataStore createStore(final StoreFactoryOptions options) {
    if (!(options instanceof FileSystemOptions)) {
      throw new AssertionError("Expected " + FileSystemOptions.class.getSimpleName());
    }
    return new FileSystemDataStore(
        (FileSystemOperations) helper.createOperations(options),
        ((FileSystemOptions) options).getStoreOptions());
  }
}
