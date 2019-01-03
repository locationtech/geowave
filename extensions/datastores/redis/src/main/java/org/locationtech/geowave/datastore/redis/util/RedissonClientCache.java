/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonClientCache {
  private static RedissonClientCache singletonInstance;

  public static synchronized RedissonClientCache getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new RedissonClientCache();
    }
    return singletonInstance;
  }

  private final LoadingCache<String, RedissonClient> clientCache =
      Caffeine.newBuilder()
          .build(
              address -> {
                final Config config = new Config();
                config
                    .useSingleServer()
                    .setConnectTimeout(15000)
                    .setTimeout(150000)
                    .setRetryInterval(15000)
                    .setAddress(address);
                return Redisson.create(config);
              });

  protected RedissonClientCache() {}

  public RedissonClient getClient(final String address) {
    return clientCache.get(address);
  }
}
