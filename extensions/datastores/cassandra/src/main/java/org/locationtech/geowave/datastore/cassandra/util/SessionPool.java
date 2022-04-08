/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.util;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class SessionPool {
  private static int DEFAULT_CQL_PORT = 9042;
  private static String DEFAULT_DATA_CENTER = "datacenter1";

  private static SessionPool singletonInstance;

  public static synchronized SessionPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new SessionPool();
    }
    return singletonInstance;
  }

  protected SessionPool() {}

  private final LoadingCache<Pair<String, String>, CqlSession> sessionCache =
      Caffeine.newBuilder().build(pair -> {
        final List<InetSocketAddress> cps =
            Arrays.stream(pair.getLeft().split(",")).filter(
                str -> (str != null) && !str.trim().isEmpty()).map(
                    SessionPool::parseSocket).collect(Collectors.toList());
        if (cps.isEmpty()) {
          return new CqlSessionBuilder().build();
        }
        return new CqlSessionBuilder().withLocalDatacenter(pair.getRight()).addContactPoints(
            cps).build();
      });

  private static InetSocketAddress parseSocket(final String str) {
    final String[] split = str.split(":");
    if (split.length == 2) {
      final int port = Integer.parseInt(split[1]);
      return new InetSocketAddress(split[0], port);
    } else if (split.length > 2) {
      throw new RuntimeException("Cannot form valid socket address from " + str);
    } else {
      return new InetSocketAddress(str, DEFAULT_CQL_PORT);
    }
  }

  public synchronized CqlSession getSession(final String contactPoints, final String datacenter) {
    final String finalDC =
        (datacenter == null) || datacenter.trim().isEmpty() ? DEFAULT_DATA_CENTER : datacenter;
    if (contactPoints == null) {
      return sessionCache.get(Pair.of("", finalDC));
    }
    return sessionCache.get(Pair.of(contactPoints, finalDC));
  }
}
