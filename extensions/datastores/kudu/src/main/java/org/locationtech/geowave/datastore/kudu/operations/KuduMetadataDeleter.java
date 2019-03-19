package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import static org.locationtech.geowave.datastore.kudu.KuduMetadataRow.KuduMetadataField;

public class KuduMetadataDeleter implements MetadataDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduMetadataDeleter.class);
  private final KuduOperations operations;
  private final MetadataType metadataType;
  private final KuduSession session;

  public KuduMetadataDeleter(final KuduOperations operations, final MetadataType metadataType) {
    this.operations = operations;
    this.metadataType = metadataType;
    this.session = operations.getSession();
  }

  @Override
  public boolean delete(final MetadataQuery query) {
    try {
      KuduTable table = operations.getTable(operations.getMetadataTableName(metadataType));
      Schema schema = table.getSchema();
      List<KuduPredicate> preds = new ArrayList<>();
      // Note: Kudu Java API requires specifying entire primary key in order to perform deletion,
      // but some parts of the primary key (i.e., primary ID, secondary ID, and timestamp) may be
      // unknown, so we instead perform the deletion using predicates on the known columns.
      if (query.hasPrimaryId()) {
        byte[] primaryId = query.getPrimaryId();
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                primaryId));
      }
      if (query.hasSecondaryId()) {
        byte[] secondaryId = query.getSecondaryId();
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                secondaryId));
      }
      for (Delete delete : operations.getDeletions(table, preds, KuduMetadataRow::new)) {
        session.apply(delete);
      }
      return true;
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
    }
    return false;
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
