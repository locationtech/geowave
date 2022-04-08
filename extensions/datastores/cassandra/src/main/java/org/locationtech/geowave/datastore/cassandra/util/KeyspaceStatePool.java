/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class KeyspaceStatePool {
  private static KeyspaceStatePool singletonInstance;

  public static synchronized KeyspaceStatePool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new KeyspaceStatePool();
    }
    return singletonInstance;
  }

  private final Map<Pair<String, String>, KeyspaceState> keyspaceStateCache = new HashMap<>();

  protected KeyspaceStatePool() {}

  public synchronized KeyspaceState getCachedState(
      final String contactPoints,
      final String keyspace) {

    final Pair<String, String> key = ImmutablePair.of(contactPoints, keyspace);
    KeyspaceState state = keyspaceStateCache.get(key);
    if (state == null) {
      state = new KeyspaceState();
      keyspaceStateCache.put(key, state);
    }
    return state;
  }

  public static class KeyspaceState {
    public final Map<String, PreparedStatement> preparedRangeReadsPerTable = new HashMap<>();
    public final Map<String, PreparedStatement> preparedRowReadPerTable = new HashMap<>();
    public final Map<String, PreparedStatement> preparedWritesPerTable = new HashMap<>();
    public final Map<String, Boolean> tableExistsCache = new HashMap<>();
  }
}
