/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.server;

import org.locationtech.geowave.core.index.ByteArray;

public interface GeoWaveColumnId {
}


class ShortColumnId implements GeoWaveColumnId {
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + columnId;
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ShortColumnId other = (ShortColumnId) obj;
    if (columnId != other.columnId) {
      return false;
    }
    return true;
  }

  private final short columnId;

  public ShortColumnId(final short columnId) {
    this.columnId = columnId;
  }
}


class ByteArrayColumnId implements GeoWaveColumnId {

  private final ByteArray columnId;

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((columnId == null) ? 0 : columnId.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ByteArrayColumnId other = (ByteArrayColumnId) obj;
    if (columnId == null) {
      if (other.columnId != null) {
        return false;
      }
    } else if (!columnId.equals(other.columnId)) {
      return false;
    }
    return true;
  }

  public ByteArrayColumnId(final ByteArray columnId) {
    this.columnId = columnId;
  }
}
