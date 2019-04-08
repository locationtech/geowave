/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.util;

import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class ClientPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientPool.class);

  private static ClientPool singletonInstance;

  public static synchronized ClientPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new ClientPool();
    }
    return singletonInstance;
  }

  private final LoadingCache<String, KuduClient> sessionCache =
      Caffeine.newBuilder().build(
          kuduMaster -> new KuduClient.KuduClientBuilder(kuduMaster).build());

  public synchronized KuduClient getClient(final String kuduMaster) {
    if (kuduMaster == null) {
      LOGGER.error("Kudu Master server must be set for Kudu");
      return null;
    }
    return sessionCache.get(kuduMaster);
  }
}
