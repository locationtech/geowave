/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;

public class BatchHandler {
  protected final CqlSession session;
  private final BatchType type = BatchType.UNLOGGED;
  protected final Map<ByteArray, BatchStatementBuilder> batches = new HashMap<>();

  public BatchHandler(final CqlSession session) {
    this.session = session;
  }

  protected BatchStatementBuilder addStatement(
      final GeoWaveRow row,
      final BatchableStatement statement) {
    final ByteArray partition = new ByteArray(row.getPartitionKey());
    BatchStatementBuilder tokenBatch = batches.get(partition);

    if (tokenBatch == null) {
      tokenBatch = new BatchStatementBuilder(type);

      batches.put(partition, tokenBatch);
    }
    synchronized (tokenBatch) {
      tokenBatch.addStatement(statement);
    }
    return tokenBatch;
  }
}
