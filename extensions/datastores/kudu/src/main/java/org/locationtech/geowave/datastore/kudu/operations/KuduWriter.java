package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduWriter implements RowWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduWriter.class);
  private final KuduOperations operations;
  private final String tableName;
  private final KuduSession session;


  public KuduWriter(String tableName, KuduOperations operations) {
    this.tableName = tableName;
    this.operations = operations;
    this.session = operations.getSession();
  }

  @Override
  public synchronized void write(GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public synchronized void write(GeoWaveRow row) {
    try {
      KuduTable table = operations.getTable(tableName);
      for (GeoWaveValue value : row.getFieldValues()) {
        KuduRow kuduRow = new KuduRow(row, value);
        Insert insert = table.newInsert();
        kuduRow.populatePartialRow(insert.getRow());
        OperationResponse resp = session.apply(insert);
        if (resp.hasRowError()) {
          LOGGER.error("Encountered error while applying insert: {}", resp.getRowError());
        }
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while writing row", e);
    }
  }

  @Override
  public synchronized void flush() {
    try {
      session.flush();
      if (session.countPendingErrors() != 0) {
        LOGGER.error(
            "Got {} pending errors while flushing Kudu session",
            session.countPendingErrors());
        for (RowError err : session.getPendingErrors().getRowErrors()) {
          LOGGER.error("{}", err);
        }
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while flushing Kudu session", e);
    }
  }

  @Override
  public synchronized void close() throws Exception {
    flush();
    session.close();
  }
}
