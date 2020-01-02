/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.time.Instant;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class GeoWaveRedisPersistedTimestampRow extends GeoWaveRedisPersistedRow {
  private final long secondsSinceEpic;
  private final int nanoOfSecond;

  public GeoWaveRedisPersistedTimestampRow(
      final short numDuplicates,
      final byte[] dataId,
      final GeoWaveValue value,
      final Instant time) {
    this(numDuplicates, dataId, value, time, null);
  }

  public GeoWaveRedisPersistedTimestampRow(
      final short numDuplicates,
      final byte[] dataId,
      final GeoWaveValue value,
      final Instant time,
      final Short duplicateId) {
    this(numDuplicates, dataId, value, time.getEpochSecond(), time.getNano(), duplicateId);
  }

  public GeoWaveRedisPersistedTimestampRow(
      final short numDuplicates,
      final byte[] dataId,
      final GeoWaveValue value,
      final long secondsSinceEpic,
      final int nanoOfSecond,
      final Short duplicateId) {
    super(numDuplicates, dataId, value, duplicateId);
    this.secondsSinceEpic = secondsSinceEpic;
    this.nanoOfSecond = nanoOfSecond;
  }

  public long getSecondsSinceEpic() {
    return secondsSinceEpic;
  }

  public int getNanoOfSecond() {
    return nanoOfSecond;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + nanoOfSecond;
    result = (prime * result) + (int) (secondsSinceEpic ^ (secondsSinceEpic >>> 32));
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final GeoWaveRedisPersistedTimestampRow other = (GeoWaveRedisPersistedTimestampRow) obj;
    if (nanoOfSecond != other.nanoOfSecond) {
      return false;
    }
    if (secondsSinceEpic != other.secondsSinceEpic) {
      return false;
    }
    return true;
  }
}
