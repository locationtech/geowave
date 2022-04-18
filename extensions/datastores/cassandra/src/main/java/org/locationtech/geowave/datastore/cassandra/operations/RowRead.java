/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

public class RowRead {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowRead.class);
  private final CassandraOperations operations;
  private final PreparedStatement preparedRead;
  private final short internalAdapterId;
  private final byte[] partitionKey;
  private final byte[] sortKey;

  protected RowRead(
      final PreparedStatement preparedRead,
      final CassandraOperations operations,
      final byte[] partitionKey,
      final byte[] sortKey,
      final Short internalAdapterId) {
    this.preparedRead = preparedRead;
    this.operations = operations;
    this.partitionKey = partitionKey;
    this.sortKey = sortKey;
    this.internalAdapterId = internalAdapterId;
  }

  public CassandraRow result() {
    if ((partitionKey != null) && (sortKey != null)) {
      try (CloseableIterator<CassandraRow> it =
          operations.executeQuery(
              preparedRead.boundStatementBuilder().set(
                  CassandraField.GW_SORT_KEY.getBindMarkerName(),
                  ByteBuffer.wrap(sortKey),
                  ByteBuffer.class).set(
                      CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
                      internalAdapterId,
                      TypeCodecs.SMALLINT).set(
                          CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
                          ByteBuffer.wrap(partitionKey),
                          ByteBuffer.class).build())) {
        if (it.hasNext()) {
          // there should only be one entry with this index
          return it.next();
        }
      }
    }
    return null;
  }
}
