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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class DynamoDBWriter implements RowWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBWriter.class);
  private static final int NUM_ITEMS = DynamoDBOperations.MAX_ROWS_FOR_BATCHWRITER;
  private static final boolean ASYNC_WRITE = false;
  private final List<WriteRequest> batchedItems = new ArrayList<>();
  private final String tableName;
  private final AmazonDynamoDBAsync client;
  private final Map<AmazonWebServiceRequest, Future<?>> futureMap = new Hashtable<>();
  private final boolean isDataIndex;

  public DynamoDBWriter(
      final AmazonDynamoDBAsync client,
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
      if (batchedItems.size() >= NUM_ITEMS) {
        do {
          writeBatch(ASYNC_WRITE);
        } while (batchedItems.size() >= NUM_ITEMS);
      }
    }
  }

  private void writeBatch(final boolean async) {
    final List<WriteRequest> batch;

    if (batchedItems.size() <= NUM_ITEMS) {
      batch = batchedItems;
    } else {
      batch = batchedItems.subList(0, NUM_ITEMS + 1);
    }
    final Map<String, List<WriteRequest>> writes = new HashMap<>();
    writes.put(tableName, new ArrayList<>(batch));
    if (async) {

      /**
       * To support asynchronous batch write a async handler is created Callbacks are provided for
       * success and error. As there might be unprocessed items on failure, they are retried
       * asynchronously Keep track of futures, so that they can be waited on during "flush"
       */
      final BatchWriteItemRequest batchRequest = new BatchWriteItemRequest(writes);
      final Future<BatchWriteItemResult> future =
          client.batchWriteItemAsync(
              batchRequest,
              new AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult>() {

                @Override
                public void onError(final Exception exception) {
                  LOGGER.warn(
                      "Unable to get response from Dynamo-Async Write " + exception.toString());
                  futureMap.remove(batchRequest);
                  return;
                }

                @Override
                public void onSuccess(
                    final BatchWriteItemRequest request,
                    final BatchWriteItemResult result) {
                  retryAsync(result.getUnprocessedItems());
                  if (futureMap.remove(request) == null) {
                    LOGGER.warn(" Unable to delete BatchWriteRequest from futuresMap ");
                  }
                }
              });

      futureMap.put(batchRequest, future);
    } else {
      final BatchWriteItemResult response =
          client.batchWriteItem(new BatchWriteItemRequest(writes));
      retry(response.getUnprocessedItems());
    }

    batch.clear();
  }

  private void retry(final Map<String, List<WriteRequest>> map) {
    for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
      for (final WriteRequest r : requests.getValue()) {
        if (r.getPutRequest() != null) {
          client.putItem(requests.getKey(), r.getPutRequest().getItem());
        }
      }
    }
  }

  private void retryAsync(final Map<String, List<WriteRequest>> map) {
    for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
      for (final WriteRequest r : requests.getValue()) {
        if (r.getPutRequest() != null) {

          /**
           * The code is pretty similar to retry. The only difference is retryAsync uses
           * putItemAsync instead of putItem
           */
          final PutItemRequest putRequest =
              new PutItemRequest(requests.getKey(), r.getPutRequest().getItem());
          final Future<PutItemResult> future =
              client.putItemAsync(putRequest, new AsyncHandler<PutItemRequest, PutItemResult>() {

                @Override
                public void onError(final Exception exception) {
                  LOGGER.warn("Putitem Async failed in Dynamo");
                  futureMap.remove(putRequest);
                }

                @Override
                public void onSuccess(final PutItemRequest request, final PutItemResult result) {
                  if (futureMap.remove(request) == null) {
                    LOGGER.warn("Unable to delete PutItemRequest from futuresMap ");
                  }

                  return;
                }
              });

          futureMap.put(putRequest, future);
        }
      }
    }
  }

  @Override
  public void flush() {
    synchronized (batchedItems) {
      while (!batchedItems.isEmpty()) {
        writeBatch(ASYNC_WRITE);
      }

      /** If its asynchronous, wait for future jobs to complete before we consider flush complete */
      for (final Future<?> future : futureMap.values()) {
        if (!future.isDone() && !future.isCancelled()) {
          try {
            future.get();
          } catch (final InterruptedException e) {
            LOGGER.error("Future interrupted", e);
          } catch (final ExecutionException e) {
            LOGGER.error("Execution exception ", e);
          }
        }
      }
    }
  }

  private static List<WriteRequest> rowToMutations(
      final GeoWaveRow row,
      final boolean isDataIndex) {
    if (isDataIndex) {
      byte[] partitionKey = DynamoDBUtils.getDynamoDBSafePartitionKey(row.getDataId());
      final Map<String, AttributeValue> map = new HashMap<>();
      map.put(
          DynamoDBRow.GW_PARTITION_ID_KEY,
          new AttributeValue().withB(ByteBuffer.wrap(partitionKey)));
      if (row.getFieldValues().length > 0) {
        // there should be exactly one value
        final GeoWaveValue value = row.getFieldValues()[0];
        if ((value.getValue() != null) && (value.getValue().length > 0)) {
          map.put(
              DynamoDBRow.GW_VALUE_KEY,
              new AttributeValue().withB(
                  ByteBuffer.wrap(DataIndexUtils.serializeDataIndexValue(value, false))));
        }
        if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
          map.put(
              DynamoDBRow.GW_VISIBILITY_KEY,
              new AttributeValue().withB(ByteBuffer.wrap(value.getVisibility())));
        }
      }
      return Collections.singletonList(new WriteRequest(new PutRequest(map)));
    } else {
      final ArrayList<WriteRequest> mutations = new ArrayList<>();
      byte[] partitionKey = DynamoDBUtils.getDynamoDBSafePartitionKey(row.getPartitionKey());

      for (final GeoWaveValue value : row.getFieldValues()) {
        final byte[] rowId = DynamoDBRow.getRangeKey(row);
        final Map<String, AttributeValue> map = new HashMap<>();

        map.put(
            DynamoDBRow.GW_PARTITION_ID_KEY,
            new AttributeValue().withB(ByteBuffer.wrap(partitionKey)));

        map.put(DynamoDBRow.GW_RANGE_KEY, new AttributeValue().withB(ByteBuffer.wrap(rowId)));

        if ((value.getFieldMask() != null) && (value.getFieldMask().length > 0)) {
          map.put(
              DynamoDBRow.GW_FIELD_MASK_KEY,
              new AttributeValue().withB(ByteBuffer.wrap(value.getFieldMask())));
        }

        if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
          map.put(
              DynamoDBRow.GW_VISIBILITY_KEY,
              new AttributeValue().withB(ByteBuffer.wrap(value.getVisibility())));
        }

        if ((value.getValue() != null) && (value.getValue().length > 0)) {
          map.put(
              DynamoDBRow.GW_VALUE_KEY,
              new AttributeValue().withB(ByteBuffer.wrap(value.getValue())));
        }

        mutations.add(new WriteRequest(new PutRequest(map)));
      }
      return mutations;
    }
  }
}
