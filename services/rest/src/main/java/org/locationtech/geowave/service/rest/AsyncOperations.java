/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** The operations started with runAsync(), by the ID their status is looked up with. */
public class AsyncOperations {
  private final ExecutorService pool = Executors.newFixedThreadPool(10);
  private final Map<String, Future<?>> operations = new ConcurrentHashMap<>();

  public String submit(final Callable<?> operation) {
    final String id = UUID.randomUUID().toString();
    operations.put(id, pool.submit(operation));
    return id;
  }

  public Future<?> get(final String id) {
    return (id == null) ? null : operations.get(id);
  }

  public void remove(final String id) {
    operations.remove(id);
  }

  public void shutdown() {
    pool.shutdownNow();
  }
}
