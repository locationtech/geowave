/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.util;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class CassandraUtils {

  // because Cassandra requires a partition key, if the geowave partition key is
  // empty, we need a non-empty constant alternative
  public static final byte[] EMPTY_PARTITION_KEY = new byte[] {-1};

  public static byte[] getCassandraSafePartitionKey(final byte[] partitionKey) {
    // Cassandra requires a non-empty partition key so we need to use a reserved byte array to
    // indicate an empty partition key
    if ((partitionKey == null) || (partitionKey.length == 0)) {
      return EMPTY_PARTITION_KEY;
    }
    return partitionKey;
  }

  public static BoundStatement[] bindInsertion(
      final PreparedStatement insertionStatement,
      final GeoWaveRow row,
      final boolean isDataIndex,
      final boolean visibilityEnabled) {
    return isDataIndex ? bindDataIndexInsertion(insertionStatement, row, visibilityEnabled)
        : bindInsertion(insertionStatement, row);
  }

  public static BoundStatement[] bindDataIndexInsertion(
      final PreparedStatement insertionStatement,
      final GeoWaveRow row,
      final boolean visibilityEnabled) {
    // the data ID becomes the partition key and the only other fields are the value and adapter ID
    final byte[] partitionKey = getCassandraSafePartitionKey(row.getDataId());
    final BoundStatement[] retVal = new BoundStatement[row.getFieldValues().length];
    int i = 0;
    for (final GeoWaveValue value : row.getFieldValues()) {
      final ByteBuffer nanoBuffer = ByteBuffer.allocate(8);
      nanoBuffer.putLong(0, Long.MAX_VALUE - System.nanoTime());
      final BoundStatementBuilder s = insertionStatement.boundStatementBuilder();
      s.set(
          CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
          ByteBuffer.wrap(partitionKey),
          ByteBuffer.class);
      s.set(CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(), row.getAdapterId(), Short.class);
      s.set(
          CassandraField.GW_VALUE_KEY.getBindMarkerName(),
          ByteBuffer.wrap(DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled)),
          ByteBuffer.class);

      retVal[i] = s.build();
      i++;
    }
    return retVal;
  }

  public static BoundStatement[] bindInsertion(
      final PreparedStatement insertionStatement,
      final GeoWaveRow row) {
    final byte[] partitionKey = getCassandraSafePartitionKey(row.getPartitionKey());
    final BoundStatement[] retVal = new BoundStatement[row.getFieldValues().length];
    int i = 0;
    for (final GeoWaveValue value : row.getFieldValues()) {
      final ByteBuffer nanoBuffer = ByteBuffer.allocate(8);
      nanoBuffer.putLong(0, Long.MAX_VALUE - System.nanoTime());
      retVal[i] =
          insertionStatement.boundStatementBuilder().set(
              CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
              ByteBuffer.wrap(partitionKey),
              ByteBuffer.class).set(
                  CassandraField.GW_SORT_KEY.getBindMarkerName(),
                  ByteBuffer.wrap(row.getSortKey()),
                  ByteBuffer.class).set(
                      CassandraField.GW_DATA_ID_KEY.getBindMarkerName(),
                      ByteBuffer.wrap(row.getDataId()),
                      ByteBuffer.class).set(
                          CassandraField.GW_FIELD_VISIBILITY_KEY.getBindMarkerName(),
                          ByteBuffer.wrap(value.getVisibility()),
                          ByteBuffer.class).set(
                              CassandraField.GW_NANO_TIME_KEY.getBindMarkerName(),
                              nanoBuffer,
                              ByteBuffer.class).set(
                                  CassandraField.GW_FIELD_MASK_KEY.getBindMarkerName(),
                                  ByteBuffer.wrap(value.getFieldMask()),
                                  ByteBuffer.class).set(
                                      CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
                                      row.getAdapterId(),
                                      Short.class).set(
                                          CassandraField.GW_VALUE_KEY.getBindMarkerName(),
                                          ByteBuffer.wrap(value.getValue()),
                                          ByteBuffer.class).set(
                                              CassandraField.GW_NUM_DUPLICATES_KEY.getBindMarkerName(),
                                              (byte) row.getNumberOfDuplicates(),
                                              byte.class).build();
      i++;
    }
    return retVal;
  }
}
