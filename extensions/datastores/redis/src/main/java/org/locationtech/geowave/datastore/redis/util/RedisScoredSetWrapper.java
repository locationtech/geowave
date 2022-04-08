/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.util.Collection;
import java.util.Iterator;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.ScoredEntry;

public class RedisScoredSetWrapper<V> extends
    AbstractRedisSetWrapper<RScoredSortedSetAsync<V>, RScoredSortedSet<V>> {

  public RedisScoredSetWrapper(
      final RedissonClient client,
      final String setName,
      final Codec codec) {
    super(client, setName, codec);
  }

  public boolean remove(final Object o) {
    return getCurrentSyncCollection().remove(o);
  }


  public Iterator<ScoredEntry<V>> entryRange(
      final double startScore,
      final boolean startScoreInclusive,
      final double endScore,
      final boolean endScoreInclusive) {
    final RScoredSortedSet<V> currentSet = getCurrentSyncCollection();
    final Collection<ScoredEntry<V>> currentResult =
        currentSet.entryRange(
            startScore,
            startScoreInclusive,
            endScore,
            endScoreInclusive,
            0,
            RedisUtils.MAX_ROWS_FOR_PAGINATION);
    if (currentResult.size() >= RedisUtils.MAX_ROWS_FOR_PAGINATION) {
      return new LazyPaginatedEntryRange<>(
          startScore,
          startScoreInclusive,
          endScore,
          endScoreInclusive,
          currentSet,
          currentResult);
    }
    return currentResult.iterator();
  }

  public void add(final double score, final V object) {
    preAdd();
    getCurrentAsyncCollection().addAsync(score, object);
  }


  public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(
      final double startScore,
      final boolean startScoreInclusive,
      final double endScore,
      final boolean endScoreInclusive) {
    return getCurrentSyncCollection().entryRangeAsync(
        startScore,
        startScoreInclusive,
        endScore,
        endScoreInclusive);
  }

  @Override
  protected RScoredSortedSetAsync<V> initAsyncCollection(
      final RBatch batch,
      final String setName,
      final Codec codec) {
    return batch.getScoredSortedSet(setName, codec);
  }

  @Override
  protected RScoredSortedSet<V> initSyncCollection(
      final RedissonClient client,
      final String setName,
      final Codec codec) {
    return client.getScoredSortedSet(setName, codec);
  }
}
