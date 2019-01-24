/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.datastore.dynamodb.operations.DynamoDBOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

public class DynamoDBDataStore extends BaseMapReduceDataStore {
  public static final String TYPE = "dynamodb";

  public DynamoDBDataStore(final DynamoDBOperations operations) {
    super(
        new IndexStoreImpl(operations, operations.getOptions().getBaseOptions()),
        new AdapterStoreImpl(operations, operations.getOptions().getBaseOptions()),
        new DataStatisticsStoreImpl(operations, operations.getOptions().getBaseOptions()),
        new AdapterIndexMappingStoreImpl(operations, operations.getOptions().getBaseOptions()),
        operations,
        operations.getOptions().getBaseOptions(),
        new InternalAdapterStoreImpl(operations));
  }
}
