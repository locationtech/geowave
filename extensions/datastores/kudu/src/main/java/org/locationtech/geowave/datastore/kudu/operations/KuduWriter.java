/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.SessionConfiguration;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.kudu.KuduDataIndexRow;
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
    setAutoFlushMode();
  }

  @Override
  public synchronized void write(GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public synchronized void write(GeoWaveRow row) {
    boolean isDataIndex = DataIndexUtils.isDataIndex(tableName);
    boolean isVisibilityEnabled = operations.options.getStoreOptions().isVisibilityEnabled();
    try {
      KuduTable table = operations.getTable(tableName);
      for (GeoWaveValue value : row.getFieldValues()) {
        Insert insert = table.newInsert();
        if (isDataIndex) {
          KuduDataIndexRow kuduRow = new KuduDataIndexRow(row, value, isVisibilityEnabled);
          kuduRow.populatePartialRow(insert.getRow());
        } else {
          KuduRow kuduRow = new KuduRow(row, value);
          kuduRow.populatePartialRow(insert.getRow());
        }
        session.apply(insert);
        if (session.getPendingErrors().getRowErrors().length > 0) {
          RowError[] rowErrors = session.getPendingErrors().getRowErrors();
          for (int i = 0; i < rowErrors.length; i++) {
            LOGGER.error("Encountered error while applying insert: {}", rowErrors[i]);
          }
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

  private boolean setAutoFlushMode() {
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    if (session.getFlushMode() != SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
      LOGGER.error("Fail to set session Flush Mode to AUTO_FLUSH_BACKGROUND.");
      return false;
    }
    return true;
  }
}
