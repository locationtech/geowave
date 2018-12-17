package org.locationtech.geowave.core.store.base.dataidx;

import java.util.concurrent.CompletableFuture;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public interface BatchDataIndexRetrieval extends DataIndexRetrieval {
  CompletableFuture<GeoWaveValue[]> getDataAsync(short adapterId, byte[] dataId);

  void flush();

  void notifyIteratorInitiated();

  void notifyIteratorExhausted();
}
