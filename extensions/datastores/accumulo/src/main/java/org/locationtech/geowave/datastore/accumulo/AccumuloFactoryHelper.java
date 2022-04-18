/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class AccumuloFactoryHelper implements StoreFactoryHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloFactoryHelper.class);

  @Override
  public StoreFactoryOptions createOptionsInstance() {
    return new AccumuloRequiredOptions();
  }

  @Override
  public DataStoreOperations createOperations(final StoreFactoryOptions options) {
    try {
      return AccumuloOperations.createOperations((AccumuloRequiredOptions) options);
    } catch (AccumuloException | AccumuloSecurityException | IOException e) {
      LOGGER.error("Unable to create Accumulo operations from config options", e);
      return null;
    }
  }
}
