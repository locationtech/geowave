/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.util;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.locationtech.geowave.datastore.dynamodb.operations.DynamoDBOperations;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDBUtils {
  // because DynamoDB requires a hash key, if the geowave partition key is
  // empty, we need a non-empty constant alternative
  public static final byte[] EMPTY_PARTITION_KEY = new byte[] {-1};

  private static final int MAX_UNPROCESSED_RETRIES = 10;
  private static final long BASE_BACKOFF_MS = 50;
  private static final long MAX_BACKOFF_MS = 5000;

  public static class NoopClosableIteratorWrapper implements Closeable {
    public NoopClosableIteratorWrapper() {}

    @Override
    public void close() {}
  }

  public static byte[] getDynamoDBSafePartitionKey(final byte[] partitionKey) {
    // DynamoDB requires a non-empty partition key so we need to use a reserved byte array to
    // indicate an empty partition key
    if ((partitionKey == null) || (partitionKey.length == 0)) {
      return EMPTY_PARTITION_KEY;
    }
    return partitionKey;
  }

  public static AttributeValue binaryValue(final byte[] bytes) {
    return AttributeValue.fromB(SdkBytes.fromByteArray(bytes));
  }

  /** The bytes of a binary attribute, or null for an attribute the item does not have. */
  public static byte[] bytes(final AttributeValue value) {
    return value == null ? null : value.b().asByteArray();
  }

  public static byte[] getPrimaryId(final Map<String, AttributeValue> map) {
    return bytes(map.get(DynamoDBOperations.METADATA_PRIMARY_ID_KEY));
  }

  public static byte[] getSecondaryId(final Map<String, AttributeValue> map) {
    return bytes(map.get(DynamoDBOperations.METADATA_SECONDARY_ID_KEY));
  }

  public static byte[] getVisibility(final Map<String, AttributeValue> map) {
    return bytes(map.get(DynamoDBOperations.METADATA_VISIBILITY_KEY));
  }

  public static byte[] getValue(final Map<String, AttributeValue> map) {
    return bytes(map.get(DynamoDBOperations.METADATA_VALUE_KEY));
  }

  /**
   * Writes a batch, resubmitting whatever DynamoDB leaves unprocessed, which it does rather than
   * fail when a table is short of capacity. The SDK's retries do not cover those items.
   */
  public static void batchWriteItem(
      final DynamoDbClient client,
      final Map<String, List<WriteRequest>> requestItems) {
    Map<String, List<WriteRequest>> unprocessed = requestItems;
    for (int retry = 0;; retry++) {
      final Map<String, List<WriteRequest>> request = unprocessed;
      unprocessed = client.batchWriteItem(b -> b.requestItems(request)).unprocessedItems();
      if (unprocessed.isEmpty()) {
        return;
      }
      backOff(retry, unprocessed.values().stream().mapToInt(List::size).sum());
    }
  }

  /** Gets a batch, resubmitting whatever keys DynamoDB leaves unprocessed. */
  public static void batchGetItem(
      final DynamoDbClient client,
      final Map<String, KeysAndAttributes> requestItems,
      final Consumer<Map<String, AttributeValue>> itemConsumer) {
    Map<String, KeysAndAttributes> unprocessed = requestItems;
    for (int retry = 0;; retry++) {
      final Map<String, KeysAndAttributes> request = unprocessed;
      final BatchGetItemResponse response = client.batchGetItem(b -> b.requestItems(request));
      response.responses().values().forEach(items -> items.forEach(itemConsumer));
      unprocessed = response.unprocessedKeys();
      if (unprocessed.isEmpty()) {
        return;
      }
      backOff(retry, unprocessed.values().stream().mapToInt(k -> k.keys().size()).sum());
    }
  }

  private static void backOff(final int retry, final int unprocessedCount) {
    if (retry >= MAX_UNPROCESSED_RETRIES) {
      throw new IllegalStateException(
          "DynamoDB still left "
              + unprocessedCount
              + " items unprocessed after "
              + MAX_UNPROCESSED_RETRIES
              + " retries");
    }
    try {
      Thread.sleep(Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS << retry));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted retrying unprocessed DynamoDB items", e);
    }
  }

  private static final String BASE64_DEFAULT_ENCODING =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
  private static final String BASE64_SORTABLE_ENCODING =
      "+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz=";

  private static final byte[] defaultToSortable = new byte[127];
  private static final byte[] sortableToDefault = new byte[127];

  static {
    Arrays.fill(defaultToSortable, (byte) 0);
    Arrays.fill(sortableToDefault, (byte) 0);
    for (int i = 0; i < BASE64_DEFAULT_ENCODING.length(); i++) {
      defaultToSortable[BASE64_DEFAULT_ENCODING.charAt(i)] =
          (byte) (BASE64_SORTABLE_ENCODING.charAt(i) & 0xFF);
      sortableToDefault[BASE64_SORTABLE_ENCODING.charAt(i)] =
          (byte) (BASE64_DEFAULT_ENCODING.charAt(i) & 0xFF);
    }
  }

  public static byte[] encodeSortableBase64(final byte[] original) {
    final byte[] bytes = Base64.getEncoder().encode(original);
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = defaultToSortable[bytes[i]];
    }
    return bytes;
  }

  public static byte[] decodeSortableBase64(final byte[] original) {
    final byte[] bytes = new byte[original.length];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = sortableToDefault[original[i]];
    }
    return Base64.getDecoder().decode(bytes);
  }
}
