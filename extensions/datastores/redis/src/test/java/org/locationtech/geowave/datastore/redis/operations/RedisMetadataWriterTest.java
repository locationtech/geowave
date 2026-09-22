/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.locationtech.geowave.datastore.redis.util.RedissonClientCache;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import redis.embedded.RedisServer;

public class RedisMetadataWriterTest {
  private static final String NAMESPACE = "RedisMetadataWriterTest";
  private static RedissonClient client;
  private static RedisServer server;

  @BeforeClass
  public static void setUp() {
    server =
        RedisServer.builder().port(6379).setting("bind 127.0.0.1").setting(
            "maxmemory 512M").setting("timeout 30000").build();
    server.start();
    client = RedissonClientCache.getInstance().getClient(null, null, "redis://127.0.0.1:6379");
  }

  @AfterClass
  public static void tearDown() {
    client.shutdown();
    server.stop();
  }

  /**
   * Deleting three rows writes three identical "count -1" deltas in quick succession. A statistic
   * value is stored as its bytes plus a timestamp in a Redis sorted set, so any two that share a
   * millisecond are one member, and DataIndexOnlyIT saw a count that fell by two instead of three.
   */
  @Test
  public void identicalStatisticDeltasAreAllKept() {
    final RScoredSortedSet<GeoWaveMetadata> set =
        RedisUtils.getMetadataSet(
            client,
            Compression.SNAPPY,
            NAMESPACE,
            MetadataType.STATISTIC_VALUES,
            false);
    set.clear();
    final GeoWaveMetadata delta =
        new GeoWaveMetadata(new byte[] {1, 2}, new byte[] {3, 4}, null, new byte[] {-1});
    final RedisMetadataWriter writer = new RedisMetadataWriter(set, true);
    final int writes = 500;
    for (int i = 0; i < writes; i++) {
      writer.write(delta);
    }
    assertEquals(writes, set.size());
    set.clear();
  }

  @Test
  public void timestampsNeverRepeat() {
    long previous = RedisMetadataWriter.nextMillis();
    for (int i = 0; i < 100_000; i++) {
      final long next = RedisMetadataWriter.nextMillis();
      assertTrue(next > previous);
      previous = next;
    }
  }
}
