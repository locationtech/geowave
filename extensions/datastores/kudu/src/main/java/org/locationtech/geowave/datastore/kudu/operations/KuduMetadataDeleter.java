package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.PartialRow;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public void close() throws Exception {}

  @Override
  public boolean delete(final MetadataQuery query) {
    try {
      Delete delete = operations.getDelete(operations.getMetadataTableName(metadataType));
      PartialRow partialRow = delete.getRow();
      byte[] primaryId = query.getPrimaryId();
      partialRow.addBinary(KuduMetadataRow.KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName(),
            primaryId);

      if (query.hasSecondaryId()) {
        byte[] secondaryId = query.getSecondaryId();
        partialRow.addBinary(
            KuduMetadataRow.KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName(),
            secondaryId);
      }
      session.apply(delete);
      return true;
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
    }
    return false;
  }

  @Override
  public void flush() {}
}
