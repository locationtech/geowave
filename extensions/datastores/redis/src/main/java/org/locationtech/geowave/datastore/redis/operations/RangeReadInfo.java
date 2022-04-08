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
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.redisson.client.protocol.ScoredEntry;
import com.google.common.primitives.UnsignedBytes;

class RangeReadInfo {
  protected byte[] partitionKey;
  protected double startScore;
  protected double endScore;
  protected byte[] explicitStartCheck, explicitEndCheck;

  public RangeReadInfo(
      final byte[] partitionKey,
      final double startScore,
      final double endScore,
      final ByteArrayRange originalRange) {
    // this is used for index rows
    this.partitionKey = partitionKey;
    this.startScore = startScore;
    this.endScore = endScore;
    explicitStartCheck =
        (originalRange.getStart() != null) && (originalRange.getStart().length > 6)
            ? Arrays.copyOfRange(originalRange.getStart(), 6, originalRange.getStart().length)
            : null;
    final byte[] end = originalRange.getEndAsNextPrefix();
    explicitEndCheck =
        (end != null) && (end.length > 6) ? Arrays.copyOfRange(end, 6, end.length) : null;
  }

  public RangeReadInfo(
      final double startScore,
      final double endScore,
      final ByteArrayRange originalRange) {
    // this is used for metadata rows
    this.startScore = startScore;
    this.endScore = endScore;
    explicitStartCheck = originalRange.getStart();
    explicitEndCheck = originalRange.getEndAsNextPrefix();
  }

  public boolean passesExplicitRowChecks(final ScoredEntry<GeoWaveRedisPersistedRow> entry) {
    final GeoWaveRedisPersistedRow row = entry.getValue();
    if ((explicitStartCheck != null)
        && (entry.getScore() == startScore)
        && (row.getSortKeyPrecisionBeyondScore().length > 0)
        && (UnsignedBytes.lexicographicalComparator().compare(
            explicitStartCheck,
            row.getSortKeyPrecisionBeyondScore()) > 0)) {
      return false;
    }
    if ((explicitEndCheck != null)
        && (entry.getScore() == endScore)
        && (row.getSortKeyPrecisionBeyondScore().length > 0)
        && (UnsignedBytes.lexicographicalComparator().compare(
            explicitEndCheck,
            row.getSortKeyPrecisionBeyondScore()) <= 0)) {
      return false;
    }
    return true;
  }

  public boolean passesExplicitMetadataRowChecks(final ScoredEntry<GeoWaveMetadata> entry) {
    final GeoWaveMetadata row = entry.getValue();
    if ((explicitStartCheck != null)
        && (entry.getScore() == startScore)
        && (row.getPrimaryId().length > 0)
        && (UnsignedBytes.lexicographicalComparator().compare(
            explicitStartCheck,
            row.getPrimaryId()) > 0)) {
      return false;
    }
    if ((explicitEndCheck != null)
        && (entry.getScore() == endScore)
        && (row.getPrimaryId().length > 0)
        && (UnsignedBytes.lexicographicalComparator().compare(
            explicitEndCheck,
            row.getPrimaryId()) <= 0)) {
      return false;
    }
    return true;
  }
}
