package org.locationtech.geowave.core.store.base.dataidx;

import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public interface DataIndexRetrieval {
  GeoWaveValue[] getData(short adapterId, byte[] dataId);
}
