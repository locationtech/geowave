package org.locationtech.geowave.datastore.foundationdb.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBMetadataTable;

public class FoundationDBMetadataWriter implements MetadataWriter {
  private final FoundationDBMetadataTable table;
  private boolean closed = false;

  public FoundationDBMetadataWriter(FoundationDBMetadataTable table) {
    this.table = table;
  }

  @Override
  public void write(GeoWaveMetadata metadata) {
    table.add(metadata);
  }

  @Override
  public void flush() {
    table.flush();
  }

  @Override
  public void close() {
    if (!closed) {
      flush();
      closed = true;
    }
  }
}
