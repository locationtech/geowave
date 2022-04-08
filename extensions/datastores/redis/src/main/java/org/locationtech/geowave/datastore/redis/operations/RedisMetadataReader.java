/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.MetadataIterators;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;
import org.redisson.client.protocol.ScoredEntry;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class RedisMetadataReader implements MetadataReader {
  private final RScoredSortedSet<GeoWaveMetadata> set;
  private final MetadataType metadataType;

  public RedisMetadataReader(
      final RScoredSortedSet<GeoWaveMetadata> set,
      final MetadataType metadataType) {
    this.set = set;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    Iterable<GeoWaveMetadata> results;
    if (query.getPrimaryId() != null) {
      if (!query.isPrefix() || (query.getPrimaryId().length > 6)) {
        // this primary ID and next prefix are going to be the same
        // score
        final double score = RedisUtils.getScore(query.getPrimaryId());
        results = set.valueRange(score, true, score, true);
      } else {
        // the primary ID prefix is short enough that we can use the
        // score of the next prefix to subset the data
        results =
            set.valueRange(
                RedisUtils.getScore(query.getPrimaryId()),
                true,
                RedisUtils.getScore(ByteArrayUtils.getNextPrefix(query.getPrimaryId())),
                false);
      }
    } else if (query.hasPrimaryIdRanges()) {
      results =
          Arrays.stream(query.getPrimaryIdRanges()).flatMap(
              range -> RedisUtils.getScoreRangesFromByteArrays(range).map(
                  scoreRange -> new RangeReadInfo(
                      scoreRange.getMinimum(),
                      scoreRange.getMaximum(),
                      range)).flatMap(r -> // Streams.stream(
              set.entryRange(
                  r.startScore,
                  true,
                  r.endScore,
                  // because we have a finite precision we need to make
                  // sure the end is inclusive and do more precise client-side
                  // filtering
                  ((r.endScore <= r.startScore) || (r.explicitEndCheck != null))).stream().filter(
                      e -> r.passesExplicitMetadataRowChecks(e)))).map(
                          ScoredEntry::getValue).collect(Collectors.toList());
    } else {
      results = set;
    }
    if (query.hasPrimaryId() || query.hasSecondaryId()) {
      results = Iterables.filter(results, new Predicate<GeoWaveMetadata>() {

        @Override
        public boolean apply(final GeoWaveMetadata input) {
          if (query.hasPrimaryId()
              && !DataStoreUtils.startsWithIfPrefix(
                  input.getPrimaryId(),
                  query.getPrimaryId(),
                  query.isPrefix())) {
            return false;
          }
          if (query.hasSecondaryId()
              && !Arrays.equals(input.getSecondaryId(), query.getSecondaryId())) {
            return false;
          }
          return true;
        }
      });
    }
    final CloseableIterator<GeoWaveMetadata> retVal;
    if (metadataType.isStatValues()) {
      retVal =
          MetadataIterators.clientVisibilityFilter(
              new CloseableIterator.Wrapper<>(RedisUtils.groupByIds(results)),
              query.getAuthorizations());
    } else {
      retVal = new CloseableIterator.Wrapper<>(results.iterator());
    }
    return retVal;
  }

}
