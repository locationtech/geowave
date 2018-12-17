package org.locationtech.geowave.core.store.base;

import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public interface GeoWaveValueStore {
  public GeoWaveValue[] getValue(byte[] dataId);
}
