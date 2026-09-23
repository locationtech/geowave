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
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

public class BatchedWriteTest {
  private static final GeoWaveRow ROW =
      new GeoWaveRowImpl(
          new GeoWaveKeyImpl(new byte[] {1}, (short) 1, new byte[] {2}, new byte[] {3}, 0),
          new GeoWaveValue[] {new GeoWaveValueImpl(new byte[] {1}, new byte[0], new byte[] {4})});

  @Test
  public void testFailedWriteFailsClose() {
    final CompletableFuture<AsyncResultSet> timedOut = new CompletableFuture<>();
    timedOut.completeExceptionally(new DriverTimeoutException("Query timed out after PT2S"));
    for (final int batchSize : new int[] {1, 50}) {
      final BatchedWrite write =
          new BatchedWrite(session(timedOut), preparedInsert(), batchSize, false, false);
      write.insert(ROW);
      final IOException e = Assert.assertThrows(IOException.class, write::close);
      Assert.assertTrue(e.getCause() instanceof DriverTimeoutException);
    }
  }

  @Test
  public void testSuccessfulWriteClosesCleanly() throws Exception {
    for (final int batchSize : new int[] {1, 50}) {
      final BatchedWrite write =
          new BatchedWrite(
              session(CompletableFuture.completedFuture(mock(AsyncResultSet.class))),
              preparedInsert(),
              batchSize,
              false,
              false);
      write.insert(ROW);
      write.close();
    }
  }

  private static CqlSession session(final CompletionStage<AsyncResultSet> result) {
    final CqlSession session = mock(CqlSession.class);
    when(session.executeAsync(any(Statement.class))).thenReturn(result);
    return session;
  }

  private static PreparedStatement preparedInsert() {
    final BoundStatementBuilder builder = mock(BoundStatementBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(mock(BoundStatement.class));
    final PreparedStatement insert = mock(PreparedStatement.class);
    when(insert.boundStatementBuilder()).thenReturn(builder);
    return insert;
  }
}
