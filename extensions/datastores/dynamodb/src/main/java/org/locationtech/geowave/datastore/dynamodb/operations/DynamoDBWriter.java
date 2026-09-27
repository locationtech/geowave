/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDBWriter implements RowWriter {
  private static final int NUM_ITEMS = DynamoDBOperations.MAX_ROWS_FOR_BATCHWRITER;
  private final List<WriteRequest> batchedItems = new ArrayList<>();
  private final String tableName;
  private final DynamoDbClient client;
  private final boolean isDataIndex;

  public DynamoDBWriter(
      final DynamoDbClient client,
      final String tableName,
      final boolean isDataIndex) {
    this.isDataIndex = isDataIndex;
    this.client = client;
    this.tableName = tableName;
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    final List<WriteRequest> mutations = new ArrayList<>();

    for (final GeoWaveRow row : rows) {
      mutations.addAll(rowToMutations(row, isDataIndex));
    }

    write(mutations);
  }

  @Override
  public void write(final GeoWaveRow row) {
    write(rowToMutations(row, isDataIndex));
  }

  public void write(final Iterable<WriteRequest> items) {
    for (final WriteRequest item : items) {
      write(item);
    }
  }

  public void write(final WriteRequest item) {
    synchronized (batchedItems) {
      batchedItems.add(item);
      while (batchedItems.size() >= NUM_ITEMS) {
        writeBatch();
      }
    }
  }

  private void writeBatch() {
    final List<WriteRequest> batch =
        batchedItems.size() <= NUM_ITEMS ? batchedItems : batchedItems.subList(0, NUM_ITEMS);
    DynamoDBUtils.batchWriteItem(
        client,
        Collections.singletonMap(tableName, new ArrayList<>(batch)));
    batch.clear();
  }

  @Override
  public void flush() {
    synchronized (batchedItems) {
      while (!batchedItems.isEmpty()) {
        writeBatch();
      }
    }
  }

  static List<WriteRequest> rowToMutations(final GeoWaveRow row, final boolean isDataIndex) {
    if (isDataIndex) {
      final byte[] partitionKey = DynamoDBUtils.getDynamoDBSafePartitionKey(row.getDataId());
      final Map<String, AttributeValue> map = new HashMap<>();
      map.put(DynamoDBRow.GW_PARTITION_ID_KEY, DynamoDBUtils.binaryValue(partitionKey));
      if (row.getFieldValues().length > 0) {
        // there should be exactly one value
        final GeoWaveValue value = row.getFieldValues()[0];
        if ((value.getValue() != null) && (value.getValue().length > 0)) {
          map.put(
              DynamoDBRow.GW_VALUE_KEY,
              DynamoDBUtils.binaryValue(DataIndexUtils.serializeDataIndexValue(value, false)));
        }
        if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
          map.put(DynamoDBRow.GW_VISIBILITY_KEY, DynamoDBUtils.binaryValue(value.getVisibility()));
        }
      }
      return Collections.singletonList(putRequest(map));
    } else {
      final ArrayList<WriteRequest> mutations = new ArrayList<>();
      final byte[] partitionKey = DynamoDBUtils.getDynamoDBSafePartitionKey(row.getPartitionKey());

      for (final GeoWaveValue value : row.getFieldValues()) {
        final byte[] rowId = DynamoDBRow.getRangeKey(row);
        final Map<String, AttributeValue> map = new HashMap<>();

        map.put(DynamoDBRow.GW_PARTITION_ID_KEY, DynamoDBUtils.binaryValue(partitionKey));

        map.put(DynamoDBRow.GW_RANGE_KEY, DynamoDBUtils.binaryValue(rowId));

        if ((value.getFieldMask() != null) && (value.getFieldMask().length > 0)) {
          map.put(DynamoDBRow.GW_FIELD_MASK_KEY, DynamoDBUtils.binaryValue(value.getFieldMask()));
        }

        if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
          map.put(DynamoDBRow.GW_VISIBILITY_KEY, DynamoDBUtils.binaryValue(value.getVisibility()));
        }

        if ((value.getValue() != null) && (value.getValue().length > 0)) {
          map.put(DynamoDBRow.GW_VALUE_KEY, DynamoDBUtils.binaryValue(value.getValue()));
        }

        mutations.add(putRequest(map));
      }
      return mutations;
    }
  }

  private static WriteRequest putRequest(final Map<String, AttributeValue> item) {
    return WriteRequest.builder().putRequest(p -> p.item(item)).build();
  }
}
