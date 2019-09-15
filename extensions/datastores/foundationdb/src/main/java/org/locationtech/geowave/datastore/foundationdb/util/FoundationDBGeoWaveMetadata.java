package org.locationtech.geowave.datastore.foundationdb.util;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class FoundationDBGeoWaveMetadata extends GeoWaveMetadata {
  private final byte[] originalKey;

  public FoundationDBGeoWaveMetadata(
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
