/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.util;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * DynamoDB leaves items unprocessed when a table runs short of capacity, and they must not drop.
 */
public class DynamoDBBatchRetryTest {
  private static final String TABLE = "table";

  private static WriteRequest put(final int i) {
    return WriteRequest.builder().putRequest(
        p -> p.item(
            Collections.singletonMap(
                "P",
                DynamoDBUtils.binaryValue(new byte[] {(byte) i})))).build();
  }

  private static Map<String, AttributeValue> key(final int i) {
    return Collections.singletonMap("P", DynamoDBUtils.binaryValue(new byte[] {(byte) i}));
  }

  private abstract static class FakeClient implements DynamoDbClient {
    @Override
    public String serviceName() {
      return SERVICE_NAME;
    }

    @Override
    public void close() {}
  }

  @Test
  public void unprocessedWritesAreResubmitted() {
    final List<List<WriteRequest>> submitted = new ArrayList<>();
    final DynamoDbClient client = new FakeClient() {
      @Override
      public BatchWriteItemResponse batchWriteItem(final BatchWriteItemRequest request) {
        final List<WriteRequest> batch = request.requestItems().get(TABLE);
        submitted.add(batch);
        // each attempt leaves its last item unprocessed
        return BatchWriteItemResponse.builder().unprocessedItems(
            batch.size() > 1
                ? Collections.singletonMap(TABLE, batch.subList(batch.size() - 1, batch.size()))
                : Collections.emptyMap()).build();
      }
    };
    DynamoDBUtils.batchWriteItem(
        client,
        Collections.singletonMap(TABLE, Arrays.asList(put(1), put(2), put(3))));
    assertEquals(
        Arrays.asList(Arrays.asList(put(1), put(2), put(3)), Arrays.asList(put(3))),
        submitted);
  }

  @Test
  public void unprocessedKeysAreResubmitted() {
    final List<List<Map<String, AttributeValue>>> submitted = new ArrayList<>();
    final DynamoDbClient client = new FakeClient() {
      @Override
      public BatchGetItemResponse batchGetItem(final BatchGetItemRequest request) {
        final List<Map<String, AttributeValue>> keys = request.requestItems().get(TABLE).keys();
        submitted.add(keys);
        // each attempt returns its first key and leaves the rest unprocessed
        final BatchGetItemResponse.Builder response =
            BatchGetItemResponse.builder().responses(
                Collections.singletonMap(TABLE, keys.subList(0, 1)));
        if (keys.size() > 1) {
          response.unprocessedKeys(
              Collections.singletonMap(
                  TABLE,
                  KeysAndAttributes.builder().keys(keys.subList(1, keys.size())).build()));
        }
        return response.build();
      }
    };
    final List<Map<String, AttributeValue>> items = new ArrayList<>();
    DynamoDBUtils.batchGetItem(
        client,
        Collections.singletonMap(
            TABLE,
            KeysAndAttributes.builder().keys(key(1), key(2), key(3)).build()),
        items::add);
    assertEquals(Arrays.asList(key(1), key(2), key(3)), items);
    assertEquals(3, submitted.size());
  }
}
