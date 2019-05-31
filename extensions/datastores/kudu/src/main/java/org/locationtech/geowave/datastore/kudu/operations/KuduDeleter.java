package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import static org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;

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
      KuduTable table = operations.getTable(tableName);
      Schema schema = table.getSchema();
      List<KuduPredicate> preds = new ArrayList<>();
      byte[] partitionKey = row.getPartitionKey();
      short adapterId = row.getAdapterId();
      byte[] sortKey = row.getSortKey();
      byte[] dataId = row.getDataId();
      int numDuplicates = row.getNumberOfDuplicates();
      // Note: Kudu Java API requires specifying entire primary key in order to perform deletion,
      // but a part of the primary key (timestamp) is unknown, so we instead perform the
      // deletion using predicates on the known columns.
      for (GeoWaveValue value : row.getFieldValues()) {
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_PARTITION_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                partitionKey));
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_ADAPTER_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                adapterId));
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_SORT_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                sortKey));
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_DATA_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                dataId));
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_NUM_DUPLICATES_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                numDuplicates));
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                value.getVisibility()));
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_FIELD_MASK_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                value.getFieldMask()));
        for (Delete delete : operations.getDeletions(table, preds, KuduRow::new)) {
          OperationResponse resp = session.apply(delete);
          if (resp.hasRowError()) {
            LOGGER.error("Encountered error while deleting row: {}", resp.getRowError());
          }
        }
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
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
        for (RowError err : session.getPendingErrors().getRowErrors()) {
          LOGGER.error("{}", err);
        }
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while flushing Kudu session", e);
    }
  }

  @Override
  public void close() {
    flush();
    try {
      session.close();
    } catch (KuduException e) {
      LOGGER.error("Encountered error while closing Kudu session", e);
    }
  }

}
