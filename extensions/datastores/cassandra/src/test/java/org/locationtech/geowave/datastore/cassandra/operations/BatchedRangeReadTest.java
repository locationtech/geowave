/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;

public class BatchedRangeReadTest {

  @Test
  public void testFailedReadFailsTheQuery() {
    final CompletableFuture<AsyncResultSet> timedOut = new CompletableFuture<>();
    timedOut.completeExceptionally(new DriverTimeoutException("Query timed out after PT2S"));
    assertQueryFails(timedOut);
  }

  @Test
  public void testFailureWhileReadingResultsFailsTheQuery() {
    final AsyncResultSet page = mock(AsyncResultSet.class);
    final Iterable<Row> rows = () -> {
      throw new DriverTimeoutException("Query timed out after PT2S");
    };
    when(page.currentPage()).thenReturn(rows);
    assertQueryFails(CompletableFuture.completedFuture(page));
  }

  @Test
  public void testEmptyReadSucceeds() {
    final AsyncResultSet page = mock(AsyncResultSet.class);
    when(page.currentPage()).thenReturn(Collections.emptyList());
    try (CloseableIterator<GeoWaveRow> results =
        read(CompletableFuture.completedFuture(page)).executeQueryAsync(mock(Statement.class))) {
      Assert.assertFalse(results.hasNext());
    }
  }

  private static void assertQueryFails(final CompletableFuture<AsyncResultSet> result) {
    try (CloseableIterator<GeoWaveRow> results =
        read(result).executeQueryAsync(mock(Statement.class))) {
      final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, () -> {
        while (results.hasNext()) {
          results.next();
        }
      });
      Assert.assertTrue(e.getCause() instanceof DriverTimeoutException);
    }
  }

  private static BatchedRangeRead<GeoWaveRow> read(final CompletableFuture<AsyncResultSet> result) {
    final CqlSession session = mock(CqlSession.class);
    when(session.executeAsync(any(Statement.class))).thenReturn(result);
    final CassandraOperations operations = mock(CassandraOperations.class);
    when(operations.getSession()).thenReturn(session);
    return new BatchedRangeRead<>(
        null,
        operations,
        new short[] {1},
        Collections.emptyList(),
        false,
        GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
        row -> true);
  }
}
