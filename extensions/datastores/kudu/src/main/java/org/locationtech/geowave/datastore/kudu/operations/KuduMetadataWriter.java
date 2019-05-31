package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduMetadataWriter implements MetadataWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduMetadataWriter.class);
  private final KuduOperations operations;
  private final KuduSession session;
  private final String tableName;

  public KuduMetadataWriter(KuduOperations operations, MetadataType metadataType) {
    this.operations = operations;
    this.session = operations.getSession();
    this.tableName = operations.getMetadataTableName(metadataType);
  }

  @Override
  public void write(final GeoWaveMetadata metadata) {
    try {
      Insert insert = operations.getTable(tableName).newInsert();
      PartialRow partialRow = insert.getRow();
      KuduMetadataRow row = new KuduMetadataRow(metadata);
      row.populatePartialRow(partialRow);
      OperationResponse resp = session.apply(insert);
      if (resp.hasRowError()) {
        LOGGER.error("Encountered error while writing metadata: {}", resp.getRowError());
      }
    } catch (KuduException e) {
      LOGGER.error("Kudu error when writing metadata", e);
    }
  }

  @Override
  public void flush() {
    try {
      session.flush();
      if (session.countPendingErrors() != 0) {
        LOGGER.error(
            "Got {} pending errors while flushing Kudu MetadataWriter session",
            session.countPendingErrors());
        for (RowError err : session.getPendingErrors().getRowErrors()) {
          LOGGER.error("{}", err);
        }
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while flushing MetadataWriter Kudu session", e);
    }
  }

  @Override
  public void close() throws Exception {
    flush();
    session.close();
  }

}
