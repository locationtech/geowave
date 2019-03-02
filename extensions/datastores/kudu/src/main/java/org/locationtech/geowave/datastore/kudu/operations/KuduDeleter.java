package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.PartialRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;
import org.locationtech.geowave.datastore.kudu.operations.KuduOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduDeleter implements RowDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduWriter.class);
  private final KuduOperations operations;
  private final String tableName;
  private final KuduSession session;

  public KuduDeleter(final KuduOperations operations, final String tableName) {
    this.operations = operations;
    this.tableName = tableName;
    this.session = operations.getSession();
  }

  @Override
  public void delete(final GeoWaveRow row) {
    try {
      for (GeoWaveValue value : row.getFieldValues()) {
        Delete delete = operations.getDelete(tableName);
        PartialRow partialRow = delete.getRow();
        operations.addToPartialRow(row, value, partialRow, null);
        session.apply(delete);
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}

}
