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
import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.redisson.api.RScoredSortedSet;
import org.redisson.client.protocol.ScoredEntry;

public class LazyPaginatedEntryRange<V> extends LazyIteratorChain<ScoredEntry<V>> {
  private final double startScore;
  private final boolean startScoreInclusive;
  private final double endScore;
  private final boolean endScoreInclusive;
  private final RScoredSortedSet<V> set;
  private Collection<ScoredEntry<V>> currentResult;
  private int currentOffset = 0;

  public LazyPaginatedEntryRange(
      final double startScore,
      final boolean startScoreInclusive,
      final double endScore,
      final boolean endScoreInclusive,
      final RScoredSortedSet<V> set,
      final Collection<ScoredEntry<V>> currentResult) {
    super();
    this.startScore = startScore;
    this.startScoreInclusive = startScoreInclusive;
    this.endScore = endScore;
    this.endScoreInclusive = endScoreInclusive;
    this.set = set;
    this.currentResult = currentResult;
  }

  @Override
  protected Iterator<? extends ScoredEntry<V>> nextIterator(final int count) {
    // the first iterator should be the initial results
    if (count == 1) {
      return currentResult.iterator();
    }
    // subsequent chained iterators will be obtained from redis
    // pagination
    if ((currentResult.size() < RedisUtils.MAX_ROWS_FOR_PAGINATION)) {
      return null;
    } else {
      currentOffset += RedisUtils.MAX_ROWS_FOR_PAGINATION;
      currentResult =
          set.entryRange(
              startScore,
              startScoreInclusive,
              endScore,
              endScoreInclusive,
              currentOffset,
              RedisUtils.MAX_ROWS_FOR_PAGINATION);
      return currentResult.iterator();
    }
  }
}
