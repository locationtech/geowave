/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Tables written by GeoWave 2.x, on AWS SDK v1, have to stay readable. The expected bytes here were
 * produced by the SDK v1 implementation.
 */
public class DynamoDBPersistedFormatTest {
  private static final short ADAPTER_ID = 7;
  private static final String TIMESTAMP = "<timestamp>";

  private static byte[] ramp(final int length, final int start, final int step) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (start + (i * step));
    }
    return bytes;
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder();
    for (final byte b : bytes) {
      sb.append(String.format("%02x", b & 0xFF));
    }
    return sb.toString();
  }

  /** The range key without the 8 bytes of write time that make each write distinct. */
  private static String rangeKeyHex(final byte[] rangeKey) {
    final String hex = hex(rangeKey);
    return hex.substring(0, hex.length() - 32) + TIMESTAMP + hex.substring(hex.length() - 16);
  }

  private static Map<String, String> itemHex(final WriteRequest request) {
    final Map<String, String> item = new TreeMap<>();
    request.putRequest().item().forEach((name, value) -> {
      final byte[] bytes = value.b().asByteArray();
      item.put(name, DynamoDBRow.GW_RANGE_KEY.equals(name) ? rangeKeyHex(bytes) : hex(bytes));
    });
    return item;
  }

  private static Map<String, String> expected(final String... nameAndHex) {
    final Map<String, String> item = new TreeMap<>();
    for (int i = 0; i < nameAndHex.length; i += 2) {
      item.put(nameAndHex[i], nameAndHex[i + 1]);
    }
    return item;
  }

  @Test
  public void sortableBase64IsUnchanged() {
    final Object[][] vectors =
        {
            {new byte[0], ""},
            {ramp(1, 0x97, 0), "5a6b3d3d"},
            {ramp(2, 0xFF, -1), "7a7a733d"},
            {ramp(3, 0x00, 1), "2b2b3230"},
            {ramp(4, 0x10, 0x31), "3232336d636b3d3d"},
            {ramp(5, 0xF0, 7), "774454792f456b3d"},
            {ramp(7, 0x80, 0x11), "553734576777484a74553d3d"},
            {ramp(9, 0x00, 0x1D), "2b2f6f754a72474666676a63"}};
    for (final Object[] vector : vectors) {
      final byte[] raw = (byte[]) vector[0];
      final byte[] encoded = DynamoDBUtils.encodeSortableBase64(raw);
      assertEquals(vector[1], hex(encoded));
      assertArrayEquals(raw, DynamoDBUtils.decodeSortableBase64(encoded));
    }
  }

  @Test
  public void queryBoundsAreUnchanged() {
    assertEquals("07002b453631", hex(DynamoDBReader.rangeStart(ADAPTER_ID, ramp(3, 0x01, 1))));
    assertEquals(
        "07002b4536317a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a6b3d3d",
        hex(DynamoDBReader.rangeEnd(ADAPTER_ID, ramp(3, 0x01, 1))));
    assertEquals(
        "07002b453631ffffffffffffffffffffffffffffffff",
        hex(DynamoDBReader.singleValueRangeEnd(ADAPTER_ID, ramp(3, 0x01, 1))));

    assertEquals(
        "07007a6a6a7378453d3d",
        hex(DynamoDBReader.rangeStart(ADAPTER_ID, ramp(4, 0xFE, -3))));
    assertEquals(
        "07007a6a6a7378547a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a773d",
        hex(DynamoDBReader.rangeEnd(ADAPTER_ID, ramp(4, 0xFE, -3))));
    assertEquals(
        "07007a6a6a7378453d3dffffffffffffffffffffffffffffffff",
        hex(DynamoDBReader.singleValueRangeEnd(ADAPTER_ID, ramp(4, 0xFE, -3))));

    assertEquals(
        "0700363142344b4b6c7a5965493d",
        hex(DynamoDBReader.rangeStart(ADAPTER_ID, ramp(8, 0x20, 0x13))));
    assertEquals(
        "0700363142344b4b6c7a59654c7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a",
        hex(DynamoDBReader.rangeEnd(ADAPTER_ID, ramp(8, 0x20, 0x13))));
    assertEquals(
        "0700363142344b4b6c7a5965493dffffffffffffffffffffffffffffffff",
        hex(DynamoDBReader.singleValueRangeEnd(ADAPTER_ID, ramp(8, 0x20, 0x13))));

    assertEquals("0700", hex(DynamoDBReader.rangeStart(ADAPTER_ID, null)));
    assertEquals(
        "0700ffffffffffffffffffffffffffffffff",
        hex(DynamoDBReader.rangeEnd(ADAPTER_ID, null)));
  }

  @Test
  public void aQueryConstrainsBothKeys() {
    final byte[] partition = ramp(2, 0x42, 1);
    final byte[] sortKey = ramp(3, 0x01, 1);
    final QueryRequest query =
        DynamoDBReader.getQuery(
            "table",
            partition,
            new ByteArrayRange(sortKey, sortKey, true),
            ADAPTER_ID);
    assertEquals("table", query.tableName());
    final Map<String, Condition> conditions = query.keyConditions();
    assertEquals(2, conditions.size());

    final Condition partitionCondition = conditions.get(DynamoDBRow.GW_PARTITION_ID_KEY);
    assertEquals(ComparisonOperator.EQ, partitionCondition.comparisonOperator());
    assertEquals(Arrays.asList(hex(partition)), hexes(partitionCondition.attributeValueList()));

    final Condition rangeCondition = conditions.get(DynamoDBRow.GW_RANGE_KEY);
    assertEquals(ComparisonOperator.BETWEEN, rangeCondition.comparisonOperator());
    assertEquals(
        Arrays.asList(
            hex(DynamoDBReader.rangeStart(ADAPTER_ID, sortKey)),
            hex(DynamoDBReader.singleValueRangeEnd(ADAPTER_ID, sortKey))),
        hexes(rangeCondition.attributeValueList()));
  }

  private static List<String> hexes(final List<AttributeValue> values) {
    return values.stream().map(v -> hex(v.b().asByteArray())).collect(Collectors.toList());
  }

  @Test
  public void indexItemsAreUnchanged() {
    final GeoWaveKeyImpl key =
        new GeoWaveKeyImpl(ramp(5, 0xA0, 3), ADAPTER_ID, ramp(2, 0x42, 1), ramp(6, 0x05, 0x2B), 2);
    assertEquals(
        "07002f482f5056663551a0a3a6a9ac" + TIMESTAMP + "0000000500000002",
        rangeKeyHex(DynamoDBRow.getRangeKey(key)));

    final GeoWaveRow row =
        new GeoWaveRowImpl(
            key,
            new GeoWaveValue[] {
                new GeoWaveValueImpl(ramp(2, 0x01, 1), ramp(3, 0x61, 1), ramp(4, 0xC0, 5)),
                new GeoWaveValueImpl(new byte[0], new byte[0], ramp(2, 0x33, 1))});
    final List<WriteRequest> items = DynamoDBWriter.rowToMutations(row, false);
    assertEquals(2, items.size());
    assertEquals(
        expected(
            "F",
            "0102",
            "P",
            "4243",
            "R",
            "07002f482f5056663551a0a3a6a9ac" + TIMESTAMP + "0000000500000002",
            "V",
            "c0c5cacf",
            "X",
            "616263"),
        itemHex(items.get(0)));
    assertEquals(
        expected(
            "P",
            "4243",
            "R",
            "07002f482f5056663551a0a3a6a9ac" + TIMESTAMP + "0000000500000002",
            "V",
            "3334"),
        itemHex(items.get(1)));
  }

  @Test
  public void anEmptyPartitionKeyIsStoredAsItsPlaceholder() {
    final GeoWaveRow row =
        new GeoWaveRowImpl(
            new GeoWaveKeyImpl(ramp(1, 0x09, 0), ADAPTER_ID, new byte[0], ramp(1, 0x77, 0), 0),
            new GeoWaveValue[] {new GeoWaveValueImpl(null, null, ramp(1, 0x55, 0))});
    final List<WriteRequest> items = DynamoDBWriter.rowToMutations(row, false);
    assertEquals(
        expected("P", "ff", "R", "0700526b3d3d09" + TIMESTAMP + "0000000100000000", "V", "55"),
        itemHex(items.get(0)));

    final DynamoDBRow read = new DynamoDBRow(items.get(0).putRequest().item());
    assertArrayEquals(new byte[0], read.getPartitionKey());
    assertArrayEquals(ramp(1, 0x77, 0), read.getSortKey());
    assertArrayEquals(ramp(1, 0x09, 0), read.getDataId());
    assertEquals(ADAPTER_ID, read.getAdapterId());
  }

  @Test
  public void dataIndexItemsAreUnchanged() {
    final GeoWaveRow row =
        new GeoWaveRowImpl(
            new GeoWaveKeyImpl(ramp(5, 0xA0, 3), ADAPTER_ID, new byte[0], new byte[0], 0),
            new GeoWaveValue[] {
                new GeoWaveValueImpl(new byte[0], ramp(3, 0x61, 1), ramp(4, 0xC0, 5))});
    final List<WriteRequest> items = DynamoDBWriter.rowToMutations(row, true);
    assertEquals(
        expected("P", "a0a3a6a9ac", "V", "c0c5cacf00", "X", "616263"),
        itemHex(items.get(0)));
  }

  @Test
  public void aStoredRowReadsBack() {
    final GeoWaveKeyImpl key =
        new GeoWaveKeyImpl(ramp(5, 0xA0, 3), ADAPTER_ID, ramp(2, 0x42, 1), ramp(6, 0x05, 0x2B), 2);
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put(DynamoDBRow.GW_PARTITION_ID_KEY, DynamoDBUtils.binaryValue(key.getPartitionKey()));
    item.put(DynamoDBRow.GW_RANGE_KEY, DynamoDBUtils.binaryValue(DynamoDBRow.getRangeKey(key)));
    item.put(DynamoDBRow.GW_VALUE_KEY, DynamoDBUtils.binaryValue(ramp(4, 0xC0, 5)));

    final DynamoDBRow row = new DynamoDBRow(item);
    assertArrayEquals(key.getPartitionKey(), row.getPartitionKey());
    assertArrayEquals(key.getSortKey(), row.getSortKey());
    assertArrayEquals(key.getDataId(), row.getDataId());
    assertEquals(ADAPTER_ID, row.getAdapterId());
    assertEquals(2, row.getNumberOfDuplicates());
    assertArrayEquals(ramp(4, 0xC0, 5), row.getFieldValues()[0].getValue());
  }

  /**
   * SDK v1's varargs setters appended, so the v1 code set the metadata key schema in two calls. Its
   * v2 builders replace, and a literal port of that would key the table on the timestamp alone.
   */
  @Test
  public void metadataTablesAreKeyedOnPrimaryIdAndTimestamp() {
    final CreateTableRequest request = DynamoDBOperations.metadataTableRequest("metadata");
    assertEquals("metadata", request.tableName());
    assertEquals(
        Arrays.asList("I:HASH", "T:RANGE"),
        request.keySchema().stream().map(k -> k.attributeName() + ":" + k.keyType()).collect(
            Collectors.toList()));
    assertEquals(
        Arrays.asList("I:B", "T:N"),
        request.attributeDefinitions().stream().map(
            a -> a.attributeName() + ":" + a.attributeType()).collect(Collectors.toList()));
    assertEquals(Long.valueOf(5), request.provisionedThroughput().readCapacityUnits());
    assertEquals(Long.valueOf(5), request.provisionedThroughput().writeCapacityUnits());
  }

  @Test
  public void indexTablesAreKeyedAsBefore() {
    final CreateTableRequest index = DynamoDBOperations.indexTableRequest("index", false, 3, 4);
    assertEquals(
        Arrays.asList("P:HASH", "R:RANGE"),
        index.keySchema().stream().map(k -> k.attributeName() + ":" + k.keyType()).collect(
            Collectors.toList()));
    assertEquals(
        Arrays.asList("P:B", "R:B"),
        index.attributeDefinitions().stream().map(
            a -> a.attributeName() + ":" + a.attributeType()).collect(Collectors.toList()));
    assertEquals(Long.valueOf(3), index.provisionedThroughput().readCapacityUnits());
    assertEquals(Long.valueOf(4), index.provisionedThroughput().writeCapacityUnits());

    final CreateTableRequest dataIndex = DynamoDBOperations.indexTableRequest("data", true, 3, 4);
    assertEquals(
        Arrays.asList("P:HASH"),
        dataIndex.keySchema().stream().map(k -> k.attributeName() + ":" + k.keyType()).collect(
            Collectors.toList()));
    assertEquals(
        Arrays.asList("P:B"),
        dataIndex.attributeDefinitions().stream().map(
            a -> a.attributeName() + ":" + a.attributeType()).collect(Collectors.toList()));
  }

  @Test
  public void anExactMetadataQueryKeepsBothAttributeValues() {
    final QueryRequest query =
        DynamoDBMetadataReader.primaryIdQuery("metadata", ramp(2, 0x10, 1), ramp(2, 0x20, 1));
    assertEquals("I = :priVal", query.keyConditionExpression());
    assertEquals("S = :secVal", query.filterExpression());
    assertEquals(2, query.expressionAttributeValues().size());
    assertEquals("1011", hex(query.expressionAttributeValues().get(":priVal").b().asByteArray()));
    assertEquals("2021", hex(query.expressionAttributeValues().get(":secVal").b().asByteArray()));

    final QueryRequest primaryOnly =
        DynamoDBMetadataReader.primaryIdQuery("metadata", ramp(2, 0x10, 1), null);
    assertEquals(null, primaryOnly.filterExpression());
    assertEquals(1, primaryOnly.expressionAttributeValues().size());
  }
}
