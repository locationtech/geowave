/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.operations;

import java.util.ArrayList;
import java.util.List;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow.KuduMetadataField;
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
    session = operations.getSession();
  }

  @Override
  public boolean delete(final MetadataQuery query) {
    final String tableName = operations.getMetadataTableName(metadataType);
    try {
      final KuduTable table = operations.getTable(tableName);
      final Schema schema = table.getSchema();
      final List<KuduPredicate> preds = new ArrayList<>();
      // Note: Kudu Java API requires specifying entire primary key in order to perform deletion,
      // but some parts of the primary key (i.e., primary ID, secondary ID, and timestamp) may be
      // unknown, so we instead perform the deletion using predicates on the known columns.
      if (query.hasPrimaryId()) {
        final byte[] primaryId = query.getPrimaryId();
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                primaryId));
      }
      if (query.hasSecondaryId()) {
        final byte[] secondaryId = query.getSecondaryId();
        preds.add(
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                secondaryId));
      }
      for (final Delete delete : operations.getDeletions(table, preds, KuduMetadataRow::new)) {
        final OperationResponse resp = session.apply(delete);
        if (resp.hasRowError()) {
          LOGGER.error("Encountered error while deleting row: {}", resp.getRowError());
        }
      }
      return true;
    } catch (final KuduException e) {
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
        for (final RowError err : session.getPendingErrors().getRowErrors()) {
          LOGGER.error("{}", err);
        }
      }
    } catch (final KuduException e) {
      LOGGER.error("Encountered error while flushing Kudu session", e);
    }
  }

  @Override
  public void close() throws Exception {
    flush();
    session.close();
  }

}
