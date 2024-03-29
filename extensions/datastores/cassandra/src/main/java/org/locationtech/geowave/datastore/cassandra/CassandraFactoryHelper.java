/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra;

import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;
import org.locationtech.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraFactoryHelper implements StoreFactoryHelper {
  @Override
  public StoreFactoryOptions createOptionsInstance() {
    return new CassandraRequiredOptions();
  }

  @Override
  public DataStoreOperations createOperations(final StoreFactoryOptions options) {
    return new CassandraOperations((CassandraRequiredOptions) options);
  }
}
