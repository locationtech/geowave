/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class RedissonClientCache {
  private static RedissonClientCache singletonInstance;

  public static synchronized RedissonClientCache getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new RedissonClientCache();
    }
    return singletonInstance;
  }

  private final LoadingCache<ClientKey, RedissonClient> clientCache =
      Caffeine.newBuilder().build(key -> {
        final Config config = new Config();
        final SingleServerConfig singleServerConfig =
            config.useSingleServer().setConnectTimeout(15000).setTimeout(150000).setRetryInterval(
                15000).setAddress(key.address);
        if (key.username != null) {
          singleServerConfig.setUsername(key.username);
        }
        if (key.password != null) {
          singleServerConfig.setPassword(key.password);
        }
        return Redisson.create(config);
      });

  protected RedissonClientCache() {}

  public RedissonClient getClient(
      final String username,
      final String password,
      final String address) {
    return clientCache.get(new ClientKey(username, password, address));
  }

  private static class ClientKey {
    private final String address;
    private final String username;
    private final String password;

    private ClientKey(final String username, final String password, final String address) {
      super();
      this.address = address;
      this.username = username;
      this.password = password;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((address == null) ? 0 : address.hashCode());
      result = (prime * result) + ((password == null) ? 0 : password.hashCode());
      result = (prime * result) + ((username == null) ? 0 : username.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final ClientKey other = (ClientKey) obj;
      if (address == null) {
        if (other.address != null) {
          return false;
        }
      } else if (!address.equals(other.address)) {
        return false;
      }
      if (password == null) {
        if (other.password != null) {
          return false;
        }
      } else if (!password.equals(other.password)) {
        return false;
      }
      if (username == null) {
        if (other.username != null) {
          return false;
        }
      } else if (!username.equals(other.username)) {
        return false;
      }
      return true;
    }
  }
}
