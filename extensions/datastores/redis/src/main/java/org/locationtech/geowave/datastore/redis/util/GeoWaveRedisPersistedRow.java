/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.util.Arrays;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class GeoWaveRedisPersistedRow {
  private final short numDuplicates;
  // optional duplicate ID to make this row unique
  private final Short duplicateId;
  private final byte[] dataId;
  private final GeoWaveValue value;

  private transient byte[] partitionKey;

  public GeoWaveRedisPersistedRow(
      final short numDuplicates,
      final byte[] dataId,
      final GeoWaveValue value) {
    this(numDuplicates, dataId, value, null);
  }

  public GeoWaveRedisPersistedRow(
      final short numDuplicates,
      final byte[] dataId,
      final GeoWaveValue value,
      final Short duplicateId) {
    this.numDuplicates = numDuplicates;
    this.dataId = dataId;
    this.value = value;
    this.duplicateId = duplicateId;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(final byte[] partitionKey) {
    this.partitionKey = partitionKey;
  }

  public short getNumDuplicates() {
    return numDuplicates;
  }

  public byte[] getDataId() {
    return dataId;
  }

  public byte[] getFieldMask() {
    return value.getFieldMask();
  }

  public byte[] getVisibility() {
    return value.getVisibility();
  }

  public byte[] getValue() {
    return value.getValue();
  }

  public GeoWaveValue getGeoWaveValue() {
    return value;
  }

  public Short getDuplicateId() {
    return duplicateId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(dataId);
    result = prime * result + ((duplicateId == null) ? 0 : duplicateId.hashCode());
    result = prime * result + numDuplicates;
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    GeoWaveRedisPersistedRow other = (GeoWaveRedisPersistedRow) obj;
    if (!Arrays.equals(dataId, other.dataId))
      return false;
    if (duplicateId == null) {
      if (other.duplicateId != null)
        return false;
    } else if (!duplicateId.equals(other.duplicateId))
      return false;
    if (numDuplicates != other.numDuplicates)
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

}
