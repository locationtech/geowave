/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.cassandra.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

public class BatchedWrite extends BatchHandler implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedWrite.class);
  // TODO: default batch size is tiny at 50 KB, reading recommendations re:
  // micro-batch writing
  // (https://dzone.com/articles/efficient-cassandra-write), we should be able
  // to gain some efficiencies for bulk ingests with batches if done
  // correctly, while other recommendations contradict this article and
  // suggest don't use batching as a performance optimization
  private static final boolean ASYNC = true;
  private final int batchSize;
  private final PreparedStatement preparedInsert;
  private static final int MAX_CONCURRENT_WRITE = 100;
  // only allow so many outstanding async reads or writes, use this semaphore
  // to control it
  private final Semaphore writeSemaphore = new Semaphore(MAX_CONCURRENT_WRITE);
  private final boolean isDataIndex;
  private final boolean visibilityEnabled;

  public BatchedWrite(
      final CqlSession session,
      final PreparedStatement preparedInsert,
      final int batchSize,
      final boolean isDataIndex,
      final boolean visibilityEnabled) {
    super(session);
    this.preparedInsert = preparedInsert;
    this.batchSize = batchSize;
    this.isDataIndex = isDataIndex;
    this.visibilityEnabled = visibilityEnabled;
  }

  public void insert(final GeoWaveRow row) {
    final BoundStatement[] statements =
        CassandraUtils.bindInsertion(preparedInsert, row, isDataIndex, visibilityEnabled);
    for (final BoundStatement statement : statements) {
      insertStatement(row, statement);
    }
  }

  private void insertStatement(final GeoWaveRow row, final BoundStatement statement) {
    if (ASYNC) {
      if (batchSize > 1) {
        final BatchStatementBuilder currentBatch = addStatement(row, statement);
        synchronized (currentBatch) {
          if (currentBatch.getStatementsCount() >= batchSize) {
            writeBatch(currentBatch);
          }
        }
      } else {
        try {
          executeAsync(statement);
        } catch (final InterruptedException e) {
          LOGGER.warn("async write semaphore interrupted", e);
          writeSemaphore.release();
        }
      }
    } else {
      session.execute(statement);
    }
  }

  private void writeBatch(final BatchStatementBuilder batch) {
    try {
      executeAsync(batch.build());

      batch.clearStatements();
    } catch (final InterruptedException e) {
      LOGGER.warn("async batch write semaphore interrupted", e);
      writeSemaphore.release();
    }
  }

  private void executeAsync(final Statement statement) throws InterruptedException {
    writeSemaphore.acquire();
    final CompletionStage<AsyncResultSet> future = session.executeAsync(statement);
    future.whenCompleteAsync((result, t) -> {
      writeSemaphore.release();
      if (t != null) {
        throw new RuntimeException(t);
      }
    });
  }

  @Override
  public void close() throws Exception {
    for (final BatchStatementBuilder batch : batches.values()) {
      synchronized (batch) {
        writeBatch(batch);
      }
    }

    // need to wait for all asynchronous batches to finish writing
    // before exiting close() method
    writeSemaphore.acquire(MAX_CONCURRENT_WRITE);
    writeSemaphore.release(MAX_CONCURRENT_WRITE);
  }
}
