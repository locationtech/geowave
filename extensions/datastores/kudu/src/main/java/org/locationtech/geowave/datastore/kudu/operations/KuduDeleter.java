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
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;
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
    session = operations.getSession();
  }

  @Override
  public void delete(final GeoWaveRow row) {
    try {
      final KuduTable table = operations.getTable(tableName);
      final Schema schema = table.getSchema();
      final List<KuduPredicate> preds = new ArrayList<>();
      final byte[] partitionKey = row.getPartitionKey();
      final short adapterId = row.getAdapterId();
      final byte[] sortKey = row.getSortKey();
      final byte[] dataId = row.getDataId();
      // Note: Kudu Java API requires specifying entire primary key in order to perform deletion,
      // but a part of the primary key (timestamp) is unknown, so we instead perform the
      // deletion using predicates on the known columns.
      for (final GeoWaveValue value : row.getFieldValues()) {
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
                schema.getColumn(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                value.getVisibility()));
        for (final Delete delete : operations.getDeletions(table, preds, KuduRow::new)) {
          final OperationResponse resp = session.apply(delete);
          if (resp.hasRowError()) {
            LOGGER.error("Encountered error while deleting row: {}", resp.getRowError());
          }
        }
      }
    } catch (final KuduException e) {
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
        for (final RowError err : session.getPendingErrors().getRowErrors()) {
          LOGGER.error("{}", err);
        }
      }
    } catch (final KuduException e) {
      LOGGER.error("Encountered error while flushing Kudu session", e);
    }
  }

  @Override
  public void close() {
    flush();
    try {
      session.close();
    } catch (final KuduException e) {
      LOGGER.error("Encountered error while closing Kudu session", e);
    }
  }

}
