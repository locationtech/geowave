package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.PartialRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import static org.locationtech.geowave.datastore.kudu.KuduRow.*;

public class KuduWriter implements RowWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduWriter.class);
  private final Object WRITE_MUTEX = new Object();
  private final KuduOperations operations;
  private final String tableName;
  private final KuduSession session;


  public KuduWriter(String tableName, KuduOperations operations) {
    this.tableName = tableName;
    this.operations = operations;
    this.session = operations.getSession();
  }

  @Override
  public void write(GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(GeoWaveRow row) {
    try {
      for (GeoWaveValue value : row.getFieldValues()) {
        ByteBuffer nanoBuffer = ByteBuffer.allocate(8);
        nanoBuffer.putLong(0, Long.MAX_VALUE - System.nanoTime());
        Insert insert = operations.getInsert(tableName);
        PartialRow partialRow = insert.getRow();
        operations.addToPartialRow(row, value, partialRow, nanoBuffer);
        session.apply(insert);
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while writing row", e);
    }
  }

  @Override
  public void flush() {
    try {
      session.flush();
      if (session.countPendingErrors() != 0) {
        LOGGER.error(
            "Got {} pending errors while flushing Kudu session",
            session.countPendingErrors());
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while flushing Kudu session", e);
    }
  }

  @Override
  public void close() throws Exception {
    flush();
    session.close();
  }
}
