package org.locationtech.geowave.core.store.adapter;

import java.util.concurrent.CompletableFuture;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;


public interface AsyncPersistenceEncoding {

  CompletableFuture<GeoWaveValue[]> getFieldValuesFuture();

}
