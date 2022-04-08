/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import java.io.IOException;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.locationtech.geowave.datastore.dynamodb.operations.DynamoDBOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBFactoryHelper implements StoreFactoryHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFactoryHelper.class);

  @Override
  public StoreFactoryOptions createOptionsInstance() {
    return new DynamoDBOptions();
  }

  @Override
  public DataStoreOperations createOperations(final StoreFactoryOptions options) {
    try {
      return DynamoDBOperations.createOperations((DynamoDBOptions) options);
    } catch (final IOException e) {
      LOGGER.error("Unable to create DynamoDB operations from config options", e);
      return null;
    }
  }
}
