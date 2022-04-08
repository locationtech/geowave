/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.IntegerCodec;
import org.redisson.client.protocol.ScoredEntry;
import redis.embedded.RedisServer;

public class RedisScoredSetWrapperTest {

  private static final String TEST_ENTRY_RANGE_NONPAGINATED_SET =
      "test_entry_range_nonpaginated_set";
  private static final String TEST_ENTRY_RANGE_PAGINATED_SET = "test_entry_range_paginated_set";
  private static final String TEST_ADD_REMOVE_SET = "test_add_remove_set";
  private static RedissonClient client;
  private static RedisServer server;

  @BeforeClass
  public static void setUp() {
    server =
        RedisServer.builder().port(6379).setting("bind 127.0.0.1").setting(
            "maxmemory 512M").setting("timeout 30000").build();
    server.start();
    client = RedissonClientCache.getInstance().getClient(null, null, "redis://127.0.0.1:6379");
    resetTestSets();
  }

  @AfterClass
  public static void tearDown() {
    resetTestSets();
    client.shutdown();
    server.stop();
  }

  private static void resetTestSets() {
    client.getScoredSortedSet(TEST_ENTRY_RANGE_NONPAGINATED_SET, IntegerCodec.INSTANCE).clear();
    client.getScoredSortedSet(TEST_ENTRY_RANGE_PAGINATED_SET, IntegerCodec.INSTANCE).clear();
    client.getScoredSortedSet(TEST_ADD_REMOVE_SET, IntegerCodec.INSTANCE).clear();
  }

  /**
   * Tests correctness of {@link RedisScoredSetWrapper#entryRange(double, boolean, double, boolean)}
   * on non-paginated output.
   */
  @Test
  public void testEntryRangeNonpaginated() {
    assertPutGet(TEST_ENTRY_RANGE_NONPAGINATED_SET, 3);
  }

  /**
   * Tests correctness of {@link RedisScoredSetWrapper#entryRange(double, boolean, double, boolean)}
   * on paginated output.
   */
  @Test
  public void testEntryRangePaginated() {
    assertPutGet(TEST_ENTRY_RANGE_PAGINATED_SET, RedisUtils.MAX_ROWS_FOR_PAGINATION + 3);
  }

  /**
   * Tests correctness of add/remove operations via {@link RedisScoredSetWrapper}.
   */
  @Test
  public void testAddRemove() throws Exception {
    /** Test using a number of entries greater than {@link AbstractRedisSetWrapper#BATCH_SIZE} */
    final int NUM_ENTRIES = 2000;
    try (RedisScoredSetWrapper<Integer> wrapper =
        new RedisScoredSetWrapper<>(client, TEST_ADD_REMOVE_SET, IntegerCodec.INSTANCE)) {
      for (int i = 0; i < NUM_ENTRIES; ++i) {
        wrapper.add(i, i);
      }
    }
    try (RedisScoredSetWrapper<Integer> wrapper =
        new RedisScoredSetWrapper<>(client, TEST_ADD_REMOVE_SET, IntegerCodec.INSTANCE)) {
      assertEquals(NUM_ENTRIES, rangeLength(wrapper.entryRange(0, true, NUM_ENTRIES, false)));
      for (int i = 0; i < NUM_ENTRIES; ++i) {
        wrapper.remove(i);
      }
      assertEquals(0, rangeLength(wrapper.entryRange(0, true, NUM_ENTRIES, false)));
    }
  }

  private <V> long rangeLength(final Iterator<ScoredEntry<V>> entryRange) {
    long numEntries = 0;
    while (entryRange.hasNext()) {
      entryRange.next();
      ++numEntries;
    }
    return numEntries;
  }

  /**
   * Asserts that {@code numEntries} entries written to {@code setName} are correctly read back
   * through {@link RedisScoredSetWrapper}.
   *
   * @param setName
   * @param numEntries
   */
  private void assertPutGet(final String setName, final int numEntries) {
    // Insertion performance degrades at larger batch sizes
    final int MAX_BATCH_SIZE = 100000;
    final RScoredSortedSet<Integer> set = client.getScoredSortedSet(setName, IntegerCodec.INSTANCE);
    final Map<Integer, Double> allEntries = new HashMap<>();
    for (int batchOffset = 0; batchOffset < numEntries; batchOffset += MAX_BATCH_SIZE) {
      final Map<Integer, Double> batchEntries = new HashMap<>();
      for (int i = 0; (i < MAX_BATCH_SIZE) && ((batchOffset + i) < numEntries); ++i) {
        batchEntries.put(batchOffset + i, (double) batchOffset + i);
      }
      set.addAll(batchEntries);
      allEntries.putAll(batchEntries);
    }

    // Check that all inserted entries are returned upon retrieval
    try (RedisScoredSetWrapper<Integer> wrapper =
        new RedisScoredSetWrapper<>(client, setName, IntegerCodec.INSTANCE)) {
      final Iterator<ScoredEntry<Integer>> results = wrapper.entryRange(0, true, numEntries, false);
      while (results.hasNext()) {
        final ScoredEntry<Integer> entry = results.next();
        assertEquals(allEntries.remove(entry.getValue()), entry.getScore(), 1e-3);
      }
      assertEquals(0, allEntries.size());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
