package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.KeyValue;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

import java.util.Iterator;

public class FoundationDBMetadataIterator extends AbstractFoundationDBIterator<GeoWaveMetadata> {
  public FoundationDBMetadataIterator(Iterator<KeyValue> it) {
    super(it);
  }

  @Override
  protected GeoWaveMetadata readRow(KeyValue keyValue) {
    return null; // TODO: map KeyValue to GeoWaveMetadata
  }
}
