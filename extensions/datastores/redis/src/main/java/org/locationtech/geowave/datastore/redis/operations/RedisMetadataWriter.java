/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.redis.util.GeoWaveTimestampMetadata;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RScoredSortedSet;

public class RedisMetadataWriter implements MetadataWriter {
  // A timestamped member is the metadata bytes plus these millis and nothing else, and the set is a
  // Redis sorted set, so two identical statistic deltas in the same millisecond are one member and
  // one delta is lost. Millis are made strictly increasing within the process, as
  // FileSystemMetadataTable and RocksDBMetadataTable do; unlike those, a writer here is created per
  // call rather than cached per table, so the guard has to be shared.
  private static final AtomicLong LAST_MILLIS = new AtomicLong();
  private final RScoredSortedSet<GeoWaveMetadata> set;
  private final boolean requiresTimestamp;

  public RedisMetadataWriter(
      final RScoredSortedSet<GeoWaveMetadata> set,
      final boolean requiresTimestamp) {
    this.set = set;
    this.requiresTimestamp = requiresTimestamp;
  }

  @Override
  public void write(final GeoWaveMetadata metadata) {
    set.add(
        RedisUtils.getScore(metadata.getPrimaryId()),
        requiresTimestamp ? new GeoWaveTimestampMetadata(metadata, nextMillis()) : metadata);
  }

  static long nextMillis() {
    final long now = System.currentTimeMillis();
    return LAST_MILLIS.updateAndGet(last -> Math.max(now, last + 1));
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws Exception {}
}
