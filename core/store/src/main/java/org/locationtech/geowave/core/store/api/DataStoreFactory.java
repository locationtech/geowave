/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;

/**
 * This is a very simple way to create a data store given an instance of that particular data
 * store's options
 */
public class DataStoreFactory {
  /**
   * Create a data store given that particular datastore implementation's options. The options
   * usually define connection parameters as well as other useful configuration particular to that
   * datastore.
   *
   * @param requiredOptions the options for the desired data store
   * @return the data store
   */
  public static DataStore createDataStore(final StoreFactoryOptions requiredOptions) {
    return new DataStorePluginOptions(requiredOptions).createDataStore();
  }
}
