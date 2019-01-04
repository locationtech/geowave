/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.index.secondary;

import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.metadata.SecondaryIndexStoreFactory;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;

public class AccumuloSecondaryIndexDataStoreFactory extends SecondaryIndexStoreFactory {
  public AccumuloSecondaryIndexDataStoreFactory(
      final String typeName,
      final String description,
      final StoreFactoryHelper helper) {
    super(typeName, description, helper);
  }

  @Override
  public SecondaryIndexDataStore createStore(final StoreFactoryOptions options) {
    if (!(options instanceof AccumuloRequiredOptions)) {
      throw new AssertionError("Expected " + AccumuloRequiredOptions.class.getSimpleName());
    }
    final AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options;
    if (opts.getStoreOptions() == null) {
      opts.setStoreOptions(new AccumuloOptions());
    }
    final DataStoreOperations accumuloOperations = helper.createOperations(opts);
    return new AccumuloSecondaryIndexDataStore(
        (AccumuloOperations) accumuloOperations,
        (AccumuloOptions) opts.getStoreOptions());
  }
}
