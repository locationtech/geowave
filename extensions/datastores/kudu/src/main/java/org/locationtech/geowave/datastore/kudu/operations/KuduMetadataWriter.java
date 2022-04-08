/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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

  public KuduMetadataWriter(final KuduOperations operations, final MetadataType metadataType) {
    this.operations = operations;
    session = operations.getSession();
    tableName = operations.getMetadataTableName(metadataType);
  }

  @Override
  public void write(final GeoWaveMetadata metadata) {
    try {
      final Insert insert = operations.getTable(tableName).newInsert();
      final PartialRow partialRow = insert.getRow();
      final KuduMetadataRow row = new KuduMetadataRow(metadata);
      row.populatePartialRow(partialRow);
      final OperationResponse resp = session.apply(insert);
      if (resp.hasRowError()) {
        LOGGER.error("Encountered error while writing metadata: {}", resp.getRowError());
      }
    } catch (final KuduException e) {
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
        for (final RowError err : session.getPendingErrors().getRowErrors()) {
          LOGGER.error("{}", err);
        }
      }
    } catch (final KuduException e) {
      LOGGER.error("Encountered error while flushing MetadataWriter Kudu session", e);
    }
  }

  @Override
  public void close() throws Exception {
    flush();
    session.close();
  }

}
