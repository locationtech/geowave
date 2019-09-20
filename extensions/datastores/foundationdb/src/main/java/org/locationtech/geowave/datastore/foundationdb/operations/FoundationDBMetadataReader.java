package org.locationtech.geowave.datastore.foundationdb.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBMetadataTable;

public class FoundationDBMetadataReader implements MetadataReader {
  private final FoundationDBMetadataTable table;
  private final MetadataType metadataType;

  public FoundationDBMetadataReader(
      final FoundationDBMetadataTable table,
      final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(MetadataQuery query) {
    return null;
  }
}
