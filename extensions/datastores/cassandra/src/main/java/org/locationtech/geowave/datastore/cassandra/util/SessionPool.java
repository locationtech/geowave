/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class SessionPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPool.class);

  private static SessionPool singletonInstance;

  public static synchronized SessionPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new SessionPool();
    }
    return singletonInstance;
  }

  protected SessionPool() {}

  private final LoadingCache<String, Session> sessionCache =
      Caffeine.newBuilder().build(
          contactPoints -> Cluster.builder().addContactPoints(
              contactPoints.split(",")).build().connect());

  public synchronized Session getSession(final String contactPoints) {
    if (contactPoints == null) {
      LOGGER.error("contact points must be set for cassandra");
      return null;
    }
    return sessionCache.get(contactPoints);
  }
}
