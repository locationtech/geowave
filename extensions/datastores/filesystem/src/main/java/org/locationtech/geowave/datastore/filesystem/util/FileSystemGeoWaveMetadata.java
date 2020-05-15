/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class FileSystemGeoWaveMetadata extends GeoWaveMetadata {
  private final byte[] originalKey;

  public FileSystemGeoWaveMetadata(
      final byte[] primaryId,
      final byte[] secondaryId,
      final byte[] visibility,
      final byte[] value,
      final byte[] originalKey) {
    super(primaryId, secondaryId, visibility, value);
    this.originalKey = originalKey;
  }

  public byte[] getKey() {
    return originalKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + getClass().hashCode();
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
    return true;
  }
}
