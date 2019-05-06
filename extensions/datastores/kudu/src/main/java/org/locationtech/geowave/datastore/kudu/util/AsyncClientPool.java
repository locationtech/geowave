/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.util;

import org.apache.kudu.client.AsyncKuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class AsyncClientPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncClientPool.class);

  private static AsyncClientPool singletonInstance;

  public static synchronized AsyncClientPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new AsyncClientPool();
    }
    return singletonInstance;
  }

  private final LoadingCache<String, AsyncKuduClient> sessionCache =
      Caffeine.newBuilder().build(
          kuduMaster -> new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster).build());

  public synchronized AsyncKuduClient getClient(final String kuduMaster) {
    if (kuduMaster == null) {
      LOGGER.error("Kudu Master server must be set for Kudu");
      return null;
    }
    return sessionCache.get(kuduMaster);
  }
}
